import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import asyncio
import aiohttp
import datetime


def get_season(response_dt):
    date_time = datetime.datetime.fromtimestamp(response_dt, tz=datetime.timezone.utc)
    month = date_time.month
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    elif month in [9, 10, 11]:
        return 'autumn'

def check_norm_city_temp(city, df, season_str, cur_temp):
    city_data = df[df['city'] == city]
    season_stats = city_data.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()
    season_mean = season_stats[season_stats['season'] == season_str]['mean'].values[0]
    season_std = season_stats[season_stats['season'] == season_str]['std'].values[0]
    lower = season_mean - 2 * season_std
    upper = season_mean + 2 * season_std
    if lower <= cur_temp <= upper:
        return f"Текущая температура {cur_temp}°C для города {city} нормальная для сезона {season_str}."
    else:
        return f"Текущая температура {cur_temp}°C для города {city} аномальная для сезона {season_str}."

async def get_lat_lon(city, API_KEY, session):
    lat_lon_url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={API_KEY}'
    async with session.get(lat_lon_url, timeout=10) as lat_lon_res:
        if lat_lon_res.status != 200:
            raise Exception(lat_lon_res.text)
        data = await lat_lon_res.json()
        if not data:
            raise Exception("Город не найден")
        lat = data[0]['lat']
        lon = data[0]['lon']
        return lat, lon
        
async def get_weather(lat, lon, API_KEY, session):
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric'
    async with session.get(url, timeout=10) as response:
        if response.status != 200:
            raise Exception(response.text)
        data = await response.json()
        cur_temp = data['main']['temp']
        response_season = get_season(data['dt'])
        req_dttm = data['dt']
    return cur_temp, response_season, req_dttm

async def get_async_weather(city, df, API_KEY, session):
    lat, lon = await get_lat_lon(city, API_KEY, session)
    cur_temp, season_str, req_dttm = await get_weather(lat, lon, API_KEY, session)
    check_info = check_norm_city_temp(city, df, season_str, cur_temp)
    return cur_temp, season_str, check_info, req_dttm

async def check_weather_async(API_KEY, df):
    cities = df['city'].unique()
    async with aiohttp.ClientSession() as session:
        tasks = [get_async_weather(city, df, API_KEY, session) for city in cities]
        weather_data = await asyncio.gather(*tasks)
    return {city: weather for city, weather in zip(cities, weather_data)}

def plot_data(df, city):
    city_data = df[df['city'] == city].copy()

    temp_stats = city_data.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()
    city_data = city_data.merge(temp_stats, on=['city', 'season'], how='left')
    city_data['lower'] = city_data['mean'] - 2 * city_data['std']
    city_data['upper'] = city_data['mean'] + 2 * city_data['std']
    anomalies = city_data[(city_data['temperature'] < city_data['lower']) | (city_data['temperature'] > city_data['upper'])]
    
    fig, ax = plt.subplots(figsize=(10,6))
    sns.lineplot(x='timestamp', y='temperature', data=city_data, ax=ax)
    sns.scatterplot(x='timestamp', y='temperature', data=anomalies, color='red', ax=ax)
    ax.set_title(f"Аномалии температур для города {city}")
    ax.set_xlabel('Дата')
    ax.set_ylabel('Температура (°C)')
    
    st.pyplot(fig)
    st.write(f'Статистика по сезонам для {city}')
    st.dataframe(temp_stats)

    fig, ax = plt.subplots(figsize=(10,6))
    ax.plot(temp_stats['season'], temp_stats['mean'], label='Средняя температура', marker='o')
    ax.fill_between(temp_stats['season'],
                    temp_stats['mean'] - temp_stats['std'],
                    temp_stats['mean'] + temp_stats['std'],
                    color='blue', alpha=0.2, label='± Стандартное отклонение')


    ax.set_title(f"Сезонный профиль температуры: {city}")
    ax.set_xlabel("Сезон")
    ax.set_ylabel("Температура, °C")
    ax.legend()
    st.pyplot(fig)


def main():
    st.title("Приложение для анализа температурных данных")
    uploaded_file = st.file_uploader("Загрузите файл с данными", type=["csv"])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        unique_cities = df['city'].unique()
        selected_cities = st.multiselect(
            "Выберите город или города:",
            options=unique_cities,
            default=unique_cities[:1]
        )
        city_data = df[df['city'].isin(selected_cities)]
        
        api_key = st.text_input("Введите ваш API-ключ для OpenWeatherMap")
        
        if api_key:
            try:
                weather_results = asyncio.run(check_weather_async(api_key, city_data))
                for city, info in weather_results.items():
                    st.write(f"{info[2]}")
            except Exception as e:
                st.error(f"Произошла ошибка: {e}")
        
        if selected_cities:
            st.write("Описательная статистика для выбранных городов:")
            filtered_data = df[df['city'].isin(selected_cities)]
            stats = filtered_data.groupby('city')['temperature'].describe()
            st.write(stats)

            st.write("Графики температур для выбранных городов:")
            for city in selected_cities:
                city_data = filtered_data[filtered_data['city'] == city]
                st.write(f"Статистика для города {city}:")
                plot_data(city_data, city)
                

if __name__ == "__main__":
    main()