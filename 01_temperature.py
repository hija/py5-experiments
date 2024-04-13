import time
import py5
import polars as pl


def load_uvindex_data(
    path: str, excel_column_identifier: str, column_title: str
) -> pl.DataFrame:
    sheet_to_df = pl.read_excel(
        path,
        sheet_id=0,
        engine="calamine",
        read_options={"header_row": 6, "use_columns": f"A,{excel_column_identifier}"},
    )

    dfs = []
    for i, (sheet, df) in enumerate(sheet_to_df.items()):
        if i == 12:
            continue
        dfs.append(df)

    month_df: pl.DataFrame = pl.concat(dfs)

    # Add day of the year column
    month_df = month_df.with_columns(DayOfYear=pl.col("Datum").dt.ordinal_day())

    # Rename the selected column
    month_df = month_df.rename(
        lambda x: column_title if x not in ["Datum", "DayOfYear"] else x
    )

    # Drop the rows where Datum is na, since those are the summary columns
    month_df = month_df.drop_nulls(subset="Datum")

    return month_df.sort(by=pl.col("Datum"), descending=False)


data = load_uvindex_data("data/S-Mitte_AfU_Halbstd.-Werte_2023.xlsx", "D", "Temperatur")


def setup():
    # 365 Tage x 48 Zeitpunkte
    py5.size(365, 48, py5.P2D)
    # py5.no_smooth() ## Cannot be disabled :(


def set_stroke_by_index(index):
    if not (index):
        py5.stroke(255, 255, 255)
    elif index <= -20:
        py5.stroke(0, 0, 0)
    elif index <= -10:
        py5.stroke(0, 0, 139)
    elif index <= 0:
        py5.stroke(0, 0, 255)
    elif index <= 20:
        py5.stroke(0, 255, 0)
    elif index <= 30:
        py5.stroke(255, 255, 0)
    elif index <= 40:
        py5.stroke(255, 0, 0)
    else:
        py5.stroke(128, 0, 128)


def draw():
    for day in range(1, 365):
        data_of_day = (
            data.filter(pl.col("DayOfYear") == day)
            .sort(by="Datum")
            .unique(subset="Datum")
        )
        uv_index_points = data_of_day["Temperatur"].to_list()
        for index, uv_index in enumerate(uv_index_points):
            set_stroke_by_index(uv_index)
            py5.point(day, index)
        py5.save("temperature_2023.png")


py5.run_sketch(block=True)
