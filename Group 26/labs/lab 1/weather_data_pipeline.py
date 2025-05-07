import requests
import csv
import pandas as pd

URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 31.5204, 74.3587  # Coordinates for Lahore, Pakistan
PARAMS = {
    "latitude": LAT,
    "longitude": LON,
    "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,precipitation_probability,surface_pressure,wind_speed_10m",
    "timezone": "auto"
}

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL, params=PARAMS)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Error fetching data: ", response.status_code)
        return None
    
    ## TODO: complete the code, the output should be data in json format


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["DateTime", "Temperature", "Humidity","Wind Speed"])
        
        times = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]
        humidities = data["hourly"]["relative_humidity_2m"]
        wind_speeds = data["hourly"]["wind_speed_10m"]
        ### TODO: complete rest of the code, HINT: write the header row and body separately

        for i in range(len(times)):
            writer.writerow([times[i], temperatures[i], humidities[i], wind_speeds[i]])
    print("Data saved to", filename)
    return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    df = pd.read_csv(input_file)

    # Apply cleaning  operations
    df["Temperature"] = df["Temperature"].apply(lambda x: x if 0 <= x <= 20 else None)
    df["Humidity"] = df["Humidity"].apply(lambda x: x if 0 <= x <= 80 else None)
    df["Wind Speed"] = df["Wind Speed"].apply(lambda x: x if 3 <= x <= 150 else None)
    
    # Drop rows with missing values
    df.dropna(inplace=True)
    df.to_csv(output_file, index=False)
    print("Cleaned data saved to", output_file)       

### Part 4. Aggregation Operation 
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header row
        data = list(reader)  # Convert CSV data to list

        # Ensure we have data
        if not data:
            print("No data available to summarize.")
            return

        # Extract values from columns
        temperatures = [float(row[1]) for row in data if row[1]]
        humidity_values = [float(row[2]) for row in data if row[2]]
        wind_speeds = [float(row[3]) for row in data if row[3]]

        # Compute statistics
        total_records = len(data)
        avg_temp = sum(temperatures) / len(temperatures)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values) / len(humidity_values)
        avg_wind_speed = sum(wind_speeds) / len(wind_speeds)

        # Print summary
        print("\n📊 Weather Data Summary 📊")
        print(f"Total Records: {total_records}")
        print(f"🌡️ Average Temperature: {avg_temp:.2f}°C")
        print(f"🔥 Max Temperature: {max_temp:.2f}°C")
        print(f"❄️ Min Temperature: {min_temp:.2f}°C")
        print(f"💧 Average Humidity: {avg_humidity:.1f}%")
        print(f"💨 Average Wind Speed: {avg_wind_speed:.2f} m/s")



if __name__ == "__main__":
    weather_data = fetch_weather_data()
    if weather_data:
        save_to_csv(weather_data, "weather_data.csv")
        print("Weather data saved to weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
        