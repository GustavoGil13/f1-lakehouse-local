def circuits_expectations(gdf):
    gdf.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    gdf.expect_column_values_to_not_be_null("circuit_key")
    gdf.expect_column_values_to_be_unique("circuit_key")
    gdf.expect_column_values_to_not_be_null("circuit_short_name")
    gdf.expect_column_values_to_not_be_null("circuit_type")


def countries_expectations(gdf):
    gdf.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    gdf.expect_column_values_to_not_be_null("country_key")
    gdf.expect_column_values_to_be_unique("country_key")
    gdf.expect_column_values_to_not_be_null("country_code")
    gdf.expect_column_values_to_not_be_null("country_name")



# gdf.expect_table_row_count_to_be_between(min_value=1, max_value=None)
# gdf.expect_column_values_to_not_be_null("meeting_key")
# gdf.expect_column_values_to_be_unique("meeting_key")
# gdf.expect_column_values_to_not_be_null("local_ts_start")
# gdf.expect_column_pair_values_A_to_be_greater_than_B("local_ts_end", "local_ts_start", or_equal=True)
# gdf.expect_column_values_to_not_be_null("ts_start")
# gdf.expect_column_pair_values_A_to_be_greater_than_B("ts_end", "ts_start", or_equal=True)
# gdf.expect_column_values_to_match_regex("country_code", r"^[A-Z]{3}$")
# gdf.expect_column_values_to_match_regex("gmt_offset", r"^[+-]?\d{2}:\d{2}:\d{2}$")



EXPECTATIONS_REGISTRY = {
    "circuits": circuits_expectations
    , "countries": countries_expectations
}
