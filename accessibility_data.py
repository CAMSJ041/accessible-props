import marimo

__generated_with = "0.14.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyodbc
    import pandas as pd
    import numpy as np
    import denodo_credentials

    # Connection details replace with your own denodo credentials
    dsn = 'DenodoODBC'
    user = denodo_credentials.DENODO_USER
    password = denodo_credentials.DENODO_PASS

    conn_str = f"DSN={dsn};UID={user};PWD={password}"
    conn = pyodbc.connect(conn_str)

    # Queries
    query_1 = """
    SELECT 
        a.assid, 
        a.ataid, 
        b.atadescription, 
        a.asaquantity
    FROM property.i_apex_asset_attribute AS a
    LEFT JOIN property.i_apex_attribute_association AS b 
        ON a.ataid = b.ataid
    WHERE a.ataid IN (
        798, -- through floor lift ("If there are any internal stairs, a through floor lift or platform stair lift will be in place")
        793, 860, 959, -- stairlifts ("If there are any internal stairs, a through floor lift or platform stair lift will be in place")
        558, 856, 1746, 1762, -- level access shower
        809, 1188, 1733, 1757 -- level access bath
    )
    """

    query_2 = """
    SELECT 
        a.assid,
        a.ataid, 
        b.atadescription, 
        a.asaquantity
    FROM property.i_apex_asset_attribute AS a
    LEFT JOIN property.i_apex_attribute_association AS b 
        ON a.ataid = b.ataid
    WHERE a.ataid IN (
        822, -- lift 1
        823, -- lift 2 ("if above the ground floor there will be at least 2 lifts")
        799, 800, 859 -- RAMPS ("step-free entry")
    )
    """

    query_3 = """
    SELECT 
        pel_pro_refno,
        dwelling_code,
        pel_ele_code,
        pel_att_code,
        pel_numeric_value,
        pel_end_date
    FROM property.i_nec_housing_property_elements
    LEFT JOIN property.cleansed_dwelling_nec ON pel_pro_refno = dwelling_reference_no
    WHERE pel_ele_code IN ('LAHR_SCORE', 'FLOORLEVEL', 'LIFT', 'STEPSIN', 'STEPSOUT','ADAPTATION')
        AND pel_end_date IS null
    """

    query_4 = """
    SELECT 
        a.dwelling_code,
        b.building_id,
        a.dwelling_occupancy_classification
    FROM property.cleansed_dwelling AS a
    LEFT JOIN property.cleansed_property_hierarchy AS b ON a.dwelling_code = b.dwelling_code
    WHERE dwelling_occupancy_classification IN ('Tenant', 'Void')
    """

    # Run queries
    apex_dwelling_data = pd.read_sql(query_1, conn)
    apex_block_data = pd.read_sql(query_2, conn)
    nec_element_data = pd.read_sql(query_3, conn)
    tenanted_dwellings = pd.read_sql(query_4, conn)

    # Close connection
    conn.close()
    return (
        apex_block_data,
        apex_dwelling_data,
        nec_element_data,
        pd,
        tenanted_dwellings,
    )


@app.cell
def _(
    apex_block_data,
    apex_dwelling_data,
    nec_element_data,
    pd,
    tenanted_dwellings,
):
    # pivot apex internal building elements so one row per dwelling and elements are columns
    dwelling_data_pivoted = apex_dwelling_data.pivot_table(
        index="assid",
        columns="atadescription",
        values="asaquantity",
        aggfunc="sum"
    )

    # pivot apex external building elements so one row per block and elements are columns
    block_data_pivoted = apex_block_data.pivot_table(
        index="assid",
        columns="atadescription",
        values="asaquantity",
        aggfunc="sum"
    )

    # assign either attribute code or numeric value as the value to use in the pivot
    nec_element_data['pel_ele_data'] = nec_element_data.apply(
        lambda row: 
            ('' if pd.isna(row['pel_att_code']) else str(row['pel_att_code'])) +
            ('' if pd.isna(row['pel_numeric_value']) else str(row['pel_numeric_value'])),
        axis=1
    )

    # replace 'NUL' text values in the field
    nec_element_data['pel_ele_data'] = nec_element_data['pel_ele_data'].replace('NUL', '', regex=True).str.strip()

    # pivot nec data so one row per dwelling and property elements as columns
    nec_data_pivoted = nec_element_data.pivot_table(
        index="dwelling_code",
        columns="pel_ele_code",
        values="pel_ele_data",
        aggfunc="first"
    )

    # merge pivoted data to the list of currently tenanted or void dwellings
    df = tenanted_dwellings.merge(dwelling_data_pivoted, left_on="dwelling_code", right_on="assid",how="left")
    df = df.merge(block_data_pivoted, left_on="building_id", right_on="assid", how="left")
    df = df.reset_index()
    nec_data_pivoted = nec_data_pivoted.reset_index()
    df = df.merge(nec_data_pivoted, on="dwelling_code", how="left")

    # create a single column for if a level access bath or shower is present
    df['level_access_bath_or_shower'] = df[
        [
            'BATH,LEVEL ACCESS',
            'BATHROOM,BATH,LEVEL ACCESS',
            'BATHROOM,SHOWER,LEVEL ACCESS',
            'SECONDARY BATHROOM,BATH,LEVEL ACCESS',
            'SECONDARY BATHROOM,SHOWER,LEVEL ACCESS',
            'SHOWER,LEVEL ACCESS,YES'
        ]
    ].sum(axis=1)
    df.loc[
        df['ADAPTATION'] == 'LEVELSHOWR',
        'level_access_bath_or_shower'
    ] = 1

    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(df):
    step_data_filtered = df[
        df["FLOORLEVEL"].notna() &
        df["STEPSIN"].notna() &
        df["STEPSOUT"].notna()
    ]
    nec_data_count = len(step_data_filtered)

    ground_floor_accessible = df[
        (df["FLOORLEVEL"] == "G") &
        (df["STEPSIN"].astype(float) == 0.0) &
        (df["STEPSOUT"].astype(float) == 0.0)
    ]
    gf_accessible_count = len(ground_floor_accessible)

    gf_la_shower = ground_floor_accessible[ground_floor_accessible["level_access_bath_or_shower"] == 1]
    gf_la_count = len(gf_la_shower)

    not_ground_floor_accessible = df[
        (~df["FLOORLEVEL"].str.contains("G", na=False)) &
        (df["STEPSIN"].astype(float) == 0.0) &
        (df["LIFT,LIFT 2"] == 1)
    ]
    not_gf_acc_count = len(not_ground_floor_accessible)

    not_gf_la_shower = not_ground_floor_accessible[not_ground_floor_accessible["level_access_bath_or_shower"] == 1]
    not_gf_la_count = len(not_gf_la_shower)

    print(f"There are {nec_data_count} tenanted or void properties in Camden's housing stock which have nec data on floor level, and number of steps inside and outside the property")
    print(f"Of the {nec_data_count} there are {gf_accessible_count} properties on the ground floor with no internal or external steps. Of those {gf_la_count} have a level-access shower or bath.")
    print(f"Of the {nec_data_count} there are {not_gf_acc_count} above the ground floor with at least 2 lifts in the block and no internal steps. However, external steps cannot be assessed as NEC counts steps to the communal entrance and stairwell steps (negated by the presence of a lift) as one value. Of those {not_gf_la_count} have a level_access shower or bath.")
    return


@app.cell
def _(df):
    LAHR_surveyed = df[
        (df["LAHR_SCORE"] != "G") &
        (df["LAHR_SCORE"].notna()) &
        (df["LAHR_SCORE"] != "")
    ]
    LAHR_count = len(LAHR_surveyed)

    LAHR_A_B = ["A", "B"]

    LAHR_AB_filtered = df[
        df["LAHR_SCORE"].isin(LAHR_A_B)
    ]
    LAHR_AB_count = len(LAHR_AB_filtered)

    AB_prop = LAHR_AB_count / LAHR_count

    print(
        f"A total of {LAHR_count} tenanted or void properties in Camden's housing stock have received LAHR surveys in the last 10 years, "
        f"of which {LAHR_AB_count} or {AB_prop:.1%} were rated as A (Wheelchair Accessible Throughout) or B (Wheelchair Accessible Essential Rooms)"
    )

    return


if __name__ == "__main__":
    app.run()
