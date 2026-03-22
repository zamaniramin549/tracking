import requests

def get_location_from_ip(ip_address):
    print(f"Scanning location data for IP: {ip_address}...\n")
    try:
        # We use a public API (ipapi.co) to fetch the location data
        url = f"https://ipapi.co/{ip_address}/json/"
        headers = {'User-Agent': 'Ethical-Hacking-Learning-Script/1.0'}
        response = requests.get(url, headers=headers)

        # Parse the JSON data returned by the website
        data = response.json()

        # Check if the API returned an error (like a private/local IP)
        if "error" in data:
            print(f"Error: {data.get('reason')}")
            return

        # Print out the location details
        print(f"City:        {data.get('city')}")
        print(f"Region:      {data.get('region')}")
        print(f"Country:     {data.get('country_name')}")
        print(f"Coordinates: {data.get('latitude')}, {data.get('longitude')}")
        print(f"ISP:         {data.get('org')}")

    except Exception as e:
        print(f"An error occurred: {e}")

# Example: Testing with Google's public DNS server
target_ip = '115.164.180.78'
get_location_from_ip(target_ip)