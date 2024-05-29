# project-1-bootcamp-group-1

## Create a virtual environment

...

Activate venv

Install requirements

`pip install -r requirements.txt`

## Run pipeline locally

from root folder

`python -m src.extract.pipelines.findwork`

# Challenges

One of the challenges we experience in this project was related to the format of the location name between the original population data and the jobs location retrieved in the findwork api responses. As most of our project questions could only be resolved after merging both datasets by their location columns it was required to standardise their location information first. 

The original `world-city-listing-table.csv` file downloaded from **(ADD SOURCE)[]** has city and country as separate columns:

| City          | Country        |
| ---           | ---            |
| Tokyo         | Japan          |
| Shanghai      | China          |
| Dhaka         | Bangladesh     |
| Sao Paulo     | Brazil         |

While the findwork responses retrieved the job locations in a single columns without a clear patters, examples of location types in this dataset are:

| Location                                          |
| ---                                               |
|'New York (NYC)'                                  |
|'Remote (Europe, US)'              
|'Berlin, Germany' 
|'Canada'
|'Stockholm' 
|'Amsterdam, Berlin, Ghent (EU) On-site/hybrid' 
|'Remote, US'
|'Hybrid (SF Bay Area, Seattle) and US Remote'
|'Dubai, United Arab Emirates' 
|'Northern Germany (Hamburg or Bremen)' 
|'US'
|'Bangalore, India' 
|'Maplewood, MN' 
|'Berlin' 
|'London, UK (Hybrid)'
|'Amsterdam, Netherlands' 
|'Hamburg, Germany (or remote EU)' 
|'UK'|

We initially thought about spliting the `location` column into two columns `city` and `country` using the first comma in the `location` column as a separator we would still end up with city names in multiple format like `New York (NYC)`, `NY` and `New York`, for example. Therefore, after a little bit of search we discovered some python libraries that could help us standardise these location fields and we decided to use `geopy` to help us with this challenge.

> geopy is a Python client for several popular geocoding web services. geopy makes it easy for Python developers to locate the coordinates of addresses, cities, countries, and landmarks across the globe using third-party geocoders and other data sources. [GeoPy’s documentation](https://geopy.readthedocs.io/en/stable/)

We wrote `_parse_location` function which uses the geolocator.geocode(location, language='en') function from `geopy` to locatate the full address of each location and then split the address into components: `city` and `country`. Find below an example of how it works

```python
# Example:
from geopy.geocoders import Nominatim
address = 'New York (NYC)'
location_geo = geolocator.geocode(address, language='en')
location_geo.address
```

Returns:

```python
'New York, United States'
```

This technique is called reverse geocoding.

However, a limitation of this approach is that it is slow and `geopy` has a rate limit of 1 request per second. It was taking about 7 minutes to run our reverse geocoding approach to the population dataset (~800 locations). Therefore, we decided to apply this standarisation step in the dataset, save the clean version and use this one instead of the original one in the pipeline extract population step. As this is a static file we believe that by doing this cleaning step did not affect the pipeline and speed up the process.
Some locations like the ones printed below could not be found, therefore, were removed before saving the result files as a csv.

| City                                      | Country        |
| ---                                       | ---            |
| leon de los aldamas                       | mexico         |
| suweon                                    | south korea    |
| durg-bhilainagar                          | india          |
| kuerle                                    | china          |
| hufuf-mubarraz                            | saudi arabia   |
| indianapolis (balance)                    | united states  |
| banghazi                                  | libya          |

The reverse geocoding technique applied to the original population dataset is in the `..` notebook.

The findwork location also have to go through the reverse geocoding step but as we do not have many locations on this response we can do it as one of the transform steps in the pipeline.

## Run dockerfile

Build and run container

`docker build -t findwork_image .`
`docker run --name findwork_container findwork_image`

Check logs

`docker logs findwork_container`

Access container

`docker exec -it findwork_container /bin/sh`

Run pipeline

`python -m extract.pipelines.findwork`

Stop and remove container

`docker stop findwork_container`
`docker rm findwork_container`
