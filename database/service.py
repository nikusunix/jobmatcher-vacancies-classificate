import uuid

from sqlalchemy import Connection, select, update, case, func

from .tables import metadata


class Vacancy:
    id: uuid.UUID
    title: str
    description: str


class ProfessionService:
    def __init__(self, connection: Connection):
        self.connection = connection

        self.vacancies_table = metadata.tables["vacancies_2025_06_13"]
        self.professions_table = metadata.tables["professions"]

    def fetch_all_professions(self) -> dict[uuid.UUID, str]:
        stmt = self.professions_table.select()

        rows = self.connection.execute(stmt).fetchall()

        return {row[0]: row[1] for row in rows}

    def upload_professions(self, professions_titles: list[str]) -> None:
        stmt = self.professions_table.insert().values(
            [
                {
                    "id": uuid.uuid4(),
                    "title": value,
                }
                for value in professions_titles
            ]
        )

        self.connection.execute(stmt)

    def get_vacancies_count(self) -> int | None:
        stmt = select(func.count()).select_from(self.vacancies_table)

        row = self.connection.execute(stmt).fetchone()

        if row:
            return row[0]

    def classify_vacancy_batch(
        self, professions: dict[uuid.UUID, str], page: int, size: int = 1000
    ):
        # Fetch vacancies batch
        stmt = (
            select(
                self.vacancies_table.c.id,
                self.vacancies_table.c.title,
                self.vacancies_table.c.description,
            )
            .order_by(
                self.vacancies_table.c.id,
            )
            .limit(size)
            .offset((page - 1) * size)
        )

        rows = self.connection.execute(stmt).fetchall()

        professtions_ids_updates: dict[uuid.UUID, uuid.UUID] = {}

        # Move into vacancies row
        for vacancy_row in rows:
            # Move into professions
            for profession_id, profession_title in professions.items():
                # Match
                if (
                    profession_title.lower() in vacancy_row.title.lower()
                    or profession_title.lower() in vacancy_row.description.lower()
                ):
                    professtions_ids_updates[vacancy_row.id] = profession_id

        # Upload updates
        stmt = self.vacancies_table.update()

        if not professtions_ids_updates:
            return

        stmt = (
            update(self.vacancies_table)
            .where(self.vacancies_table.c.id.in_(professtions_ids_updates.keys()))
            .values(
                profession_id=case(
                    {
                        vacancy_id: profession_id
                        for vacancy_id, profession_id in professtions_ids_updates.items()
                    },
                    value=self.vacancies_table.c.id,
                )
            )
        )

        self.connection.execute(stmt)
