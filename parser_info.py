import asyncio

import aiohttp

from create_connection import SQLiteConnection


url = (
    'https://api.uktradeinfo.com/OTS? ' +
    '$expand=Commodity($select=Cn8Code,Cn8LongDescription),' +
    'Country($select=Area1a,CountryName),' +
    'Port($select=PortName),' +
    'FlowType($select=FlowTypeDescription),' +
    'Date($select=Year,MonthName) ' +
    '& $filter=Monthid eq 202112 ' +
    '&$select=Value,NetMass,SuppUnit&$skip='
)


def insert_into_db(values: list[dict]):
    with SQLiteConnection() as cursor:
        for element in values:
            cn8code = element.get('Commodity').get('Cn8Code')
            cn8longdescription = (
                element
                .get('Commodity')
                .get('Cn8LongDescription')
            )
            flowtype = (
                element
                .get('FlowType')
                .get('FlowTypeDescription')
                .strip()
            )
            if flowtype.startswith('EU'):
                eu_non_eu = 'EU'
            else:
                eu_non_eu = 'NON EU'
            continent = element.get('Country').get('Area1a')
            country = element.get('Country').get('CountryName')
            portname = element.get('Port').get('PortName')
            value = element.get('Value')
            netmass = element.get('NetMass')
            suppunit = element.get('SuppUnit')
            year = element.get('Date').get('Year')
            month = element.get('Date').get('MonthName')
            cursor.execute(
                """
                INSERT INTO UKTrade (
                    Cn8Code,
                    Cn8LongDescription,
                    "EU/NonEU",
                    Continent,
                    Country,
                    PortName,
                    "Value (£)",
                    "NetMass (Kg)",
                    SuppUnit,
                    FlowType,
                    Year,
                    Month
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cn8code, cn8longdescription, eu_non_eu, continent,
                    country, portname, value, netmass, suppunit, flowtype,
                    year, month
                )
            )


async def gather_info(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            json_text = await response.json()
            values = json_text.get('value')
            if values:
                insert_into_db(values)
                skip = url.split('$')[-1]
                print(f'{skip} закончил работу.')
                return 1
            else:
                return None


async def main():
    index = 0
    while True:
        tasks_array = []
        for _ in range(5):
            skip = str(index * 30000)
            tasks_array.append(asyncio.create_task(gather_info(url+skip)))
            index += 1
        results = await asyncio.gather(*tasks_array)

        if any(result is None for result in results):
            print('Программа завершила работу.')
            break


asyncio.run(main())
