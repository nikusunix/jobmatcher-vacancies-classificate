import math
from uuid import UUID
import rich

from typing import Set
from rich.progress import Progress
from concurrent.futures import ProcessPoolExecutor, as_completed

from database.connection import engine
from database.tables import metadata
from database.service import ProfessionService
from services import CSVParser

BATCH_SIZE = 500
MAX_WORKERS = 8


def get_professions_titles() -> Set[str]:
    parser = CSVParser('./file.csv')
    return parser.parse()

def upload_professions(professions: Set[str]) -> None:
     # Reflect tables
    metadata.reflect(bind=engine)

    with engine.connect() as connection:
        profesions_service = ProfessionService(connection)

        profesions_service.upload_professions(list(professions))
    
        connection.commit()

def upload_professions_from_file() -> None:
    rich.print("Read file...")
    titles = get_professions_titles()
    
    rich.print("Upload to database...")
    upload_professions(titles)

    rich.print("Done")

def classify_vacancies_professions_process(page: int, professions: dict[UUID, str]) -> int:
    metadata.reflect(bind=engine)

    with engine.connect() as conn:
        service = ProfessionService(conn)
        
        # Process
        service.classify_vacancy_batch(professions, page=page, size=BATCH_SIZE)
        
        # Commit
        conn.commit()
    
        return page

def classify_vacancies_professions():
     # Reflect tables
    metadata.reflect(bind=engine)

    with engine.connect() as connection:
        profesions_service = ProfessionService(connection)
        
        # Fetch count 
        vacancies_count = profesions_service.get_vacancies_count()

        if not vacancies_count:
            raise Exception("Can't to fetch vacancies couint :()")

        professions = profesions_service.fetch_all_professions()

        if not professions:
            raise Exception("Can't to fetch professions list")
        
        batches_count = math.ceil(vacancies_count / BATCH_SIZE)
        batches_nums = list(range(1, batches_count + 1))
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Vacancies classification in progress...", total=batches_count)
                
            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(classify_vacancies_professions_process, batch_num, professions) for batch_num in batches_nums}

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        rich.print(f"[red]Batch failed: {e}")
                    finally:
                        progress.advance(task)

            rich.print("[green]Task completed.")

classify_vacancies_professions()
