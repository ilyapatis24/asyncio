import asyncio
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import Integer, Column, String, Text


PG_DSN = 'postgresql+asyncpg://user:password@127.0.0.1:5432/db_name'
engine = create_async_engine(PG_DSN)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

ALL_PEOPLE = 84


class SwapiPeople(Base):
    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(Text)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(Text)
    starships = Column(Text)
    vehicles = Column(Text)


async def get_url(url, key, session):
    async with session.get(f'{url}') as response:
        data = await response.json()
        return data[key]


async def get_urls(urls, key, session):
    tasks = (asyncio.create_task(get_url(url, key, session)) for url in urls)
    for task in tasks:
        yield await task


async def get_data(urls, key, session):
    data_list = []
    async for el in get_urls(urls, key, session):
        data_list.append(el)
    return ', '.join(data_list)


async def paste_to_db(people_data):
    async with Session() as session:
        async with ClientSession() as client_session:
            for character_data in people_data:
                if character_data is not None:
                    homeworld = await get_data([character_data['homeworld']], 'name', client_session)
                    films = await get_data(character_data['films'], 'title', client_session)
                    species = await get_data(character_data['species'], 'name', client_session)
                    starships = await get_data(character_data['starships'], 'name', client_session)
                    vehicles = await get_data(character_data['vehicles'], 'name', client_session)
                    character_data = SwapiPeople(
                        birth_year=character_data['birth_year'],
                        eye_color=character_data['eye_color'],
                        gender=character_data['gender'],
                        hair_color=character_data['hair_color'],
                        height=character_data['height'],
                        mass=character_data['mass'],
                        name=character_data['name'],
                        skin_color=character_data['skin_color'],
                        homeworld=homeworld,
                        films=films,
                        species=species,
                        starships=starships,
                        vehicles=vehicles,
                    )
                    session.add(character_data)
                    await session.commit()


async def get_character(people_id: int, session: ClientSession):
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        if response.ok:
            character = await response.json()
            return character
        else:
            pass


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async with ClientSession() as session:
        coro = [get_character(people_id, session=session) for people_id in range(1, ALL_PEOPLE)]
        people = await asyncio.gather(*coro)
        asyncio.create_task(paste_to_db(people))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


if __name__ == '__main__':
    asyncio.run(main())
