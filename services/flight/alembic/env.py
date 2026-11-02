from alembic import context
from sqlalchemy import create_engine, pool
import os

config = context.config
url = os.environ["FLIGHT_DATABASE_URL"]

def run_migrations_online():
    engine = create_engine(url, poolclass=pool.NullPool)
    with engine.connect() as conn:
        context.configure(connection=conn, target_metadata=None, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()

run_migrations_online()
