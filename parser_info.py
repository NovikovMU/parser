import asyncio
import os

import aiohttp
import pandas as pd


url = (
    'https://api.uktradeinfo.com/OTS? ' +
    '$expand=Commodity($select=Cn8Code,Cn8LongDescription),' +
    'Country($select=Area1a,CountryName),' +
    'Port($select=PortName),' +
    'FlowType($select=FlowTypeDescription),' +
    'Date($select=Year,MonthName) ' +
    '& $filter=Monthid eq 202112 and (FlowTypeId eq 1 or FlowTypeId eq 2) ' +
    '&$select=Value,NetMass,SuppUnit&$skip='
)

file_path = 'output.csv'
headers = [
    'Cn8Code',
    'Cn8LongDescription',
    'EU/NonEU',
    'Continent',
    'Country',
    'PortName',
    'Value (£)',
    'NetMass (Kg)',
    'SuppUnit',
    'FlowType',
    'Year',
    'Month'
]


def insert_into_db(values: list[dict]):
    data_list = []
    for element in values:
        Cn8Code = element.get('Commodity').get('Cn8Code')
        if not Cn8Code or len(Cn8Code.strip('-')) != 8:
            continue
        data_dict = {
            'Cn8Code': Cn8Code,
            'Cn8LongDescription': (
                element
                .get('Commodity')
                .get('Cn8LongDescription')
            ),
            'Continent': element.get('Country').get('Area1a'),
            'Country': element.get('Country').get('CountryName'),
            'PortName': element.get('Port').get('PortName'),
            'Value (£)': element.get('Value'),
            'NetMass (Kg)': element.get('NetMass'),
            'SuppUnit': element.get('SuppUnit'),
            'FlowType': (
                element
                .get('FlowType')
                .get('FlowTypeDescription')
                .strip()
            ),
            'Year': element.get('Date').get('Year'),
            'Month': element.get('Date').get('MonthName')

        }
        if data_dict.get('FlowType').startswith('EU'):
            data_dict['EU/NonEU'] = 'EU'
        else:
            data_dict['EU/NonEU'] = 'NonEU'
        data_list.append(data_dict)
    df = pd.DataFrame(data_list)
    df = df.reindex(columns=headers)
    if not os.path.isfile(file_path):
        df.to_csv(file_path, index=False, mode='a', header=True, sep=';')
    else:
        df.to_csv(file_path, index=False, mode='a', header=False, sep=';')


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
            task = (
                asyncio.create_task(
                    asyncio.wait_for(gather_info(url+skip), timeout=None))
            )
            tasks_array.append(task)
            index += 1
        results = await asyncio.gather(*tasks_array)

        if any(result is None for result in results):
            print('Программа завершила работу.')
            break


asyncio.run(main())
