import asyncio
import aiohttp
import datetime
from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def paste_to_db(persons_json):
    async with Session() as session:
        orm_objects = []
        for i, item in enumerate(persons_json):
            tasks = [
                get_string_param(item["films"], "title"),
                get_string_param(item["species"], "name"),
                get_string_param(item["starships"], "name"),
                get_string_param(item["vehicles"], "name"),
            ]

            result = await asyncio.gather(*tasks)

            s = SwapiPeople(
                id_pers=i,
                birth_year=item["birth_year"],
                eye_color=item["eye_color"],
                films=result[0],
                gender=item["gender"],
                hair_color=item["hair_color"],
                height=item["height"],
                homeworld=item["homeworld"],
                mass=item["mass"],
                name=item["name"],
                skin_color=item["skin_color"],
                species=result[1],
                starships=result[2],
                vehicles=result[3],
            )
            orm_objects.append(s)
        session.add_all(orm_objects)
        await session.commit()


async def get_param(url: str, param: str):
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
        json_data = await response.json()
        await session.close()
        return json_data[param]


async def get_string_param(urls: list, param: str):
    task_list = (get_param(url, param) for url in urls)

    data = await asyncio.gather(*task_list)

    return ", ".join(data)


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    person_coros = (get_people(i) for i in range(1, 83))

    person_coros_chunked = chunked(person_coros, 5)

    for person_coros_chunk in person_coros_chunked:
        persons = await asyncio.gather(*person_coros_chunk)
        asyncio.create_task(paste_to_db(persons))

    tasks = asyncio.all_tasks() - {
        asyncio.current_task(),
    }
    await asyncio.gather(*tasks)
    # tasks = [get_param("https://swapi.dev/api/films/1/",'title' )]
    # result = await asyncio.gather(*tasks)
    # print(result)


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
