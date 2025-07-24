from sqlalchemy import MetaData, Table

metadata = MetaData()

vacancies_table = Table("vacancies_2025_06_13", metadata, schema="public")
professions_table = Table("professions", metadata, schema="public")
