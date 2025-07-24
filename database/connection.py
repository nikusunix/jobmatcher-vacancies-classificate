from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg://jobmatcher_admin:9oa4LIdXIN0eX8@95.173.103.101:56701/jobmatcher/development",
)
