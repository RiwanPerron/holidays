import pandas as pd
import glob


def create_date_range(start_date, end_date):
    return pd.date_range(start=start_date, end=end_date, freq='D')


def import_and_filter_subdivisions(country):
    department_list_path = f"data/src/{country}/subdivisions.csv"
    department_list = pd.read_csv(department_list_path, delimiter=";", keep_default_na=False, na_values=[])

    filters = {
        'fr': "EN department",
        'es': "EN province",
        'de': "DE Bundesland",
        'it': "EN province"
    }

    category_filter = filters[country]


    department_list_filtered = \
    department_list[department_list['Category'].str.contains(category_filter, na=False, case=False)][
        ["Name", "Country", "Parent","ShortName"]].copy()

    if country in ['es', 'de']:
        department_list_filtered['Name'] = department_list_filtered['Name'].str.split(',').apply(
            lambda x: next((item.replace("EN ", "") for item in x if item.startswith('EN ')), None))

    else :
        department_list_filtered['Name'] = department_list_filtered['Name'].str.replace(f'{country.upper()} ', '',
                                                                                    regex=False)

    return department_list_filtered


def load_and_prepare_holidays(country_code):
    public_holidays_path = f"data/src/{country_code}/holidays/holidays.public.csv"
    public_holidays = pd.read_csv(public_holidays_path, delimiter=";", keep_default_na=False, na_values=[],
                                  parse_dates=['StartDate', 'EndDate'])

    school_holidays_files = glob.glob(f"data/src/{country_code}/holidays/*holidays.school*.csv")
    school_holidays = pd.concat([pd.read_csv(file, delimiter=";", keep_default_na=False, na_values=[],
                                             parse_dates=['StartDate', 'EndDate']) for file in school_holidays_files],
                                ignore_index=True)

    school_holidays['Subdivisions'] = school_holidays['Subdivisions'].str.split(',')
    public_holidays['Subdivisions'] = public_holidays['Subdivisions'].str.split(',')

    school_holidays['EndDate'] = school_holidays['EndDate'].fillna(school_holidays['StartDate'])
    public_holidays['EndDate'] = public_holidays['EndDate'].fillna(public_holidays['StartDate'])

    def process_holiday_names(holidays_df, holiday_type):
        holidays_df[holiday_type] = holidays_df['Name'].str.split(',').apply(
            lambda x: next((item.replace("EN ", "") for item in x if item.startswith('EN ')), None))
        return holidays_df

    school_holidays = process_holiday_names(school_holidays, 'School_Holiday_name')
    public_holidays = process_holiday_names(public_holidays, 'Public_Holiday_name')


    return public_holidays, school_holidays



def check_school_holidays(df, school_holidays,scope):

    school_holidays_expanded = school_holidays.explode('Subdivisions').reset_index(drop=True)
    school_holidays_expanded['Subdivisions'] = school_holidays_expanded['Subdivisions'].astype(str)

    if scope == 'Regional':
        school_holiday_check = pd.merge(df[["Name", "Parent", "departure_date","ShortName"]], school_holidays_expanded[
            ["Subdivisions", "StartDate", "EndDate", "School_Holiday_name"]],
                                        left_on=['Parent'], right_on='Subdivisions', how='left')

        school_holiday_check.rename(columns={'School_Holiday_name': 'School_Holiday_name_regional'}, inplace=True)


    elif scope == 'Provincial':
        school_holiday_check = pd.merge(df[["Name", "Parent", "departure_date","ShortName"]], school_holidays_expanded[
            ["Subdivisions", "StartDate", "EndDate", "School_Holiday_name"]],
                                        left_on=['ShortName'], right_on='Subdivisions', how='left')

        school_holiday_check.rename(columns={'School_Holiday_name': 'School_Holiday_name_provincial'}, inplace=True)

    return school_holiday_check[
        (school_holiday_check['departure_date'] >= school_holiday_check['StartDate']) &
        (school_holiday_check['departure_date'] <= school_holiday_check['EndDate'])].copy()


def check_public_holidays(df, public_holidays, scope):
    if scope == 'National':
        holiday_check = pd.merge(df[["Name", "Parent", "departure_date"]],
                                 public_holidays[public_holidays['RegionalScope'] == scope][
                                     ["StartDate","EndDate", "Public_Holiday_name"]],
                                 how='cross')
        holiday_check = holiday_check[ (holiday_check['departure_date'] >= holiday_check['StartDate']) &
        (holiday_check['departure_date'] <= holiday_check['EndDate'])].copy()
        holiday_check.rename(columns={'Public_Holiday_name': 'Public_Holiday_name_national'}, inplace=True)

    elif scope == 'Regional':
        holiday_check = pd.merge(df[["Name", "Parent", "departure_date"]],
                                 public_holidays[public_holidays['RegionalScope'] == scope].explode(
                                     'Subdivisions').reset_index(drop=True)[
                                     ["Subdivisions", "StartDate","EndDate", "Public_Holiday_name"]],
                                 left_on='Parent', right_on='Subdivisions', how='inner')
        holiday_check.rename(columns={'Public_Holiday_name': 'Public_Holiday_name_regional'}, inplace=True)
        holiday_check = holiday_check[(holiday_check['departure_date'] >= holiday_check['StartDate']) &
                                      (holiday_check['departure_date'] <= holiday_check['EndDate'])].copy()
    return holiday_check


def add_holiday_info(df, country_code):
    df['departure_date'] = pd.to_datetime(df['departure_date'])

    public_holidays, school_holidays = load_and_prepare_holidays(country_code)


    school_holiday_regional_check = check_school_holidays(df, school_holidays, 'Regional')
    school_holiday_provincial_check = check_school_holidays(df, school_holidays, 'Provincial')

    public_national_holiday_check = check_public_holidays(df, public_holidays, 'National')
    public_regional_holiday_check = check_public_holidays(df, public_holidays, 'Regional')

    holidays_df = pd.merge(df[["Name", "departure_date","ShortName"]],
                           school_holiday_regional_check[["Name", "departure_date", "School_Holiday_name_regional"]],
                           on=['Name', 'departure_date'], how='outer')

    holidays_df = pd.merge(holidays_df,
                           school_holiday_provincial_check[["Name", "departure_date", "School_Holiday_name_provincial"]],
                           on=['Name', 'departure_date'], how='outer')

    holidays_df = pd.merge(holidays_df,
                           public_regional_holiday_check[["Name", "departure_date", "Public_Holiday_name_regional"]],
                           on=['Name', 'departure_date'], how='outer')

    holidays_df = pd.merge(holidays_df,
                           public_national_holiday_check[["Name", "departure_date", "Public_Holiday_name_national"]],
                           on=['Name', 'departure_date'], how='outer')

    return holidays_df


def generate_holidays(country_code):
    df = import_and_filter_subdivisions(country_code)
    df_dates = pd.DataFrame({'departure_date': create_date_range("2025-01-01", "2026-12-31")})
    df = pd.merge(df, df_dates, how='cross').reset_index(drop=True)

    updated_df = add_holiday_info(df, country_code)

    updated_df.to_csv(f'{country_code}_holidays.csv', index=False)

    print(f"Data has been refreshed for : {country_code}")


if __name__ == "__main__":
    generate_holidays("es")
    generate_holidays("fr")
