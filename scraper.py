"""
Provides queries against the FTS API, fetching JSON and translating it into pandas dataframes.
It unfortunately doesn't show the structure of the returned data explicitly, that's all handled by pandas.
At some point we may want to create dedicated classes for each type of data returned by the API, to do validation etc,
but then we'll also need to implement join logic between these classes.
"""

import pandas as pd
import os


FTS_BASE_URL = 'http://fts.unocha.org/api/v1/'
JSON_SUFFIX = '.json'


def fetch_json_as_dataframe(url):
    return pd.read_json(url)


def fetch_json_as_dataframe_with_id(url):
    dataframe = fetch_json_as_dataframe(url)
    if 'id' in dataframe.columns:
        return dataframe.set_index('id')
    else:
        return dataframe  # happens with an empty result


def build_json_url(middle_part):
    return FTS_BASE_URL + middle_part + JSON_SUFFIX


def convert_date_columns_from_string_to_timestamp(dataframe, column_names):
    for column_name in column_names:
        dataframe[column_name] = dataframe[column_name].apply(pd.datetools.parse)


def fetch_sectors_json_as_dataframe():
    return fetch_json_as_dataframe_with_id(build_json_url('Sector'))


def fetch_countries_json_as_dataframe():
    return fetch_json_as_dataframe_with_id(build_json_url('Country'))


def fetch_organizations_json_as_dataframe():
    return fetch_json_as_dataframe_with_id(build_json_url('Organization'))


def fetch_emergencies_json_for_country_as_dataframe(country):
    """
    This accepts both names ("Slovakia") and ISO country codes ("SVK")
    """
    return fetch_json_as_dataframe_with_id(build_json_url('Emergency/country/' + country))


def fetch_emergencies_json_for_year_as_dataframe(year):
    return fetch_json_as_dataframe_with_id(build_json_url('Emergency/year/' + str(year)))


def fetch_appeals_json_as_dataframe_given_url(url):
    dataframe = fetch_json_as_dataframe_with_id(url)
    convert_date_columns_from_string_to_timestamp(dataframe, ['start_date', 'end_date', 'launch_date'])
    return dataframe


def fetch_appeals_json_for_country_as_dataframe(country):
    """
    This accepts both names ("Slovakia") and ISO country codes ("SVK")
    """
    return fetch_appeals_json_as_dataframe_given_url(build_json_url('Appeal/country/' + country))


def fetch_appeals_json_for_year_as_dataframe(year):
    return fetch_appeals_json_as_dataframe_given_url(build_json_url('Appeal/year/' + str(year)))


def fetch_projects_json_for_appeal_as_dataframe(appeal_id):
    dataframe = fetch_json_as_dataframe_with_id(build_json_url('Project/appeal/' + str(appeal_id)))
    if not dataframe.empty:  # guard against empty result
        convert_date_columns_from_string_to_timestamp(dataframe, ['end_date', 'last_updated_datetime'])
    return dataframe


def fetch_clusters_json_for_appeal_as_dataframe(appeal_id):
    # NOTE no id present in this data
    return fetch_json_as_dataframe(build_json_url('Cluster/appeal/' + str(appeal_id)))


def fetch_contributions_json_as_dataframe_given_url(url):
    dataframe = fetch_json_as_dataframe_with_id(url)
    if not dataframe.empty:  # guard against empty result
        convert_date_columns_from_string_to_timestamp(dataframe, ['decision_date'])
    return dataframe


def fetch_contributions_json_for_appeal_as_dataframe(appeal_id):
    return fetch_contributions_json_as_dataframe_given_url(build_json_url('Contribution/appeal/' + str(appeal_id)))


def fetch_contributions_json_for_emergency_as_dataframe(emergency_id):
    return fetch_contributions_json_as_dataframe_given_url(
        build_json_url('Contribution/emergency/' + str(emergency_id)))


def fetch_grouping_type_json_for_appeal_as_dataframe(middle_part, appeal_id, grouping=None, alias=None):
    """
    Grouping can be one of:
        Donor
        Recipient
        Sector
        Emergency
        Appeal
        Country
        Cluster
    Alias is used to name the grouping type column and use it as an index.
    """
    url = build_json_url(middle_part) + '?Appeal=' + str(appeal_id)

    if grouping:
        url += '&GroupBy=' + grouping

    # NOTE no id present in this data
    raw_dataframe = fetch_json_as_dataframe(url)

    # oddly the JSON of interest is nested inside the "grouping" element
    processed_frame = pd.DataFrame.from_records(raw_dataframe.grouping.values)

    if alias:
        processed_frame = processed_frame.rename(columns={'type': alias, 'amount': middle_part})
        processed_frame = processed_frame.set_index(alias)

    return processed_frame


def fetch_funding_json_for_appeal_as_dataframe(appeal_id, grouping=None, alias=None):
    """
    Committed or contributed funds, including carry over from previous years
    """
    return fetch_grouping_type_json_for_appeal_as_dataframe("funding", appeal_id, grouping, alias)


def fetch_pledges_json_for_appeal_as_dataframe(appeal_id, grouping=None, alias=None):
    """
    Contains uncommitted pledges, not funding that has already processed to commitment or contribution stages
    """
    return fetch_grouping_type_json_for_appeal_as_dataframe("pledges", appeal_id, grouping, alias)


if __name__ == "__main__":
    # test various fetch commands (requires internet connection)
    country = 'Chad'
    appeal_id = 942

    print fetch_sectors_json_as_dataframe()
    print fetch_emergencies_json_for_country_as_dataframe(country)
    print fetch_projects_json_for_appeal_as_dataframe(appeal_id)
    print fetch_funding_json_for_appeal_as_dataframe(appeal_id)



"""
Can be used to produce the following CSV files for upload into CKAN:
  - sectors.csv
  - countries.csv
  - organizations.csv
  - emergencies.csv (for a given country)
  - appeals.csv (for a given country)
  - projects.csv (for a given country, based on appeals)
  - contributions.csv (for given country, based on emergencies, which should capture all appeals, also)
"""

# TODO extract strings to header section above the code


def build_csv_path(base_path, object_type, country=None):
    """
    Using CSV names that duplicate the file paths here, which generally I don't like,
    but having very explicit filenames is maybe nicer to sort out for CKAN.
    """
    filename = 'fts_' + object_type + '.csv'

    if country:  # a little bit of duplication but easier to read
        filename = 'fts_' + country + '_' + object_type + '.csv'

    return os.path.join(base_path, filename)


def write_dataframe_to_csv(dataframe, path):
    print "Writing", path
    # include the index which is an ID for each of the objects serialized by this script
    # use Unicode as many non-ASCII characters present in this data
    dataframe.to_csv(path, index=True, encoding='utf-8')


def filter_out_empty_dataframes(dataframes):
    # empty dataframes will fail the "if" test
    return [frame for frame in dataframes if not frame.empty]


def produce_sectors_csv(output_dir):
    sectors = fts_queries.fetch_sectors_json_as_dataframe()
    write_dataframe_to_csv(sectors, build_csv_path(output_dir, 'sectors'))


def produce_countries_csv(output_dir):
    countries = fts_queries.fetch_countries_json_as_dataframe()
    write_dataframe_to_csv(countries, build_csv_path(output_dir, 'countries'))


def produce_organizations_csv(output_dir):
    organizations = fts_queries.fetch_organizations_json_as_dataframe()
    write_dataframe_to_csv(organizations, build_csv_path(output_dir, 'organizations'))


def produce_global_csvs(base_output_dir):
    # not sure if this directory creation code should be somewhere else..?
    output_dir = os.path.join(base_output_dir, 'fts', 'global')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    produce_sectors_csv(output_dir)
    produce_countries_csv(output_dir)
    produce_organizations_csv(output_dir)


def produce_emergencies_csv_for_country(output_dir, country):
    emergencies = fts_queries.fetch_emergencies_json_for_country_as_dataframe(country)
    write_dataframe_to_csv(emergencies, build_csv_path(output_dir, 'emergencies', country=country))


def produce_appeals_csv_for_country(output_dir, country):
    appeals = fts_queries.fetch_appeals_json_for_country_as_dataframe(country)
    write_dataframe_to_csv(appeals, build_csv_path(output_dir, 'appeals', country=country))


def produce_projects_csv_for_country(output_dir, country):
    # first get all appeals for this country (could eliminate this duplicative call, but it's not expensive)
    appeals = fts_queries.fetch_appeals_json_for_country_as_dataframe(country)
    appeal_ids = appeals.index
    # then get all projects corresponding to those appeals and concatenate into one big frame
    list_of_projects = [fts_queries.fetch_projects_json_for_appeal_as_dataframe(appeal_id) for appeal_id in appeal_ids]
    list_of_non_empty_projects = filter_out_empty_dataframes(list_of_projects)
    projects_frame = pd.concat(list_of_non_empty_projects)
    write_dataframe_to_csv(projects_frame, build_csv_path(output_dir, 'projects', country=country))


def produce_contributions_csv_for_country(output_dir, country):
    # first get all emergencies for this country (could eliminate this duplicative call, but it's not expensive)
    emergencies = fts_queries.fetch_emergencies_json_for_country_as_dataframe(country)
    emergency_ids = emergencies.index
    # then get all contributions corresponding to those emergencies and concatenate into one big frame
    list_of_contributions = [fts_queries.fetch_contributions_json_for_emergency_as_dataframe(emergency_id)
                             for emergency_id in emergency_ids]
    list_of_non_empty_contributions = filter_out_empty_dataframes(list_of_contributions)
    contributions_master_frame = pd.concat(list_of_non_empty_contributions)
    write_dataframe_to_csv(contributions_master_frame, build_csv_path(output_dir, 'contributions', country=country))


def produce_csvs_for_country(base_output_dir, country):
    output_dir = os.path.join(base_output_dir, 'fts', 'per_country', country)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    produce_emergencies_csv_for_country(output_dir, country)
    produce_appeals_csv_for_country(output_dir, country)
    produce_projects_csv_for_country(output_dir, country)
    produce_contributions_csv_for_country(output_dir, country)


if __name__ == "__main__":
    # output all CSVs for the given countries to '/tmp/'
    country_codes = ['COL', 'SSD', 'YEM', 'PAK']  # the set of starter countries for DAP
    tmp_output_dir = '/tmp/'

    produce_global_csvs(tmp_output_dir)
    for country_code in country_codes:
        produce_csvs_for_country(tmp_output_dir, country_code)
