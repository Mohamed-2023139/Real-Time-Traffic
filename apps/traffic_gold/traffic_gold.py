from dim_road import build_dim_road
from dim_zone import build_dim_zone
from dim_weather import build_dim_weather
from dim_date import build_dim_date
from fact_traffic import build_fact_traffic


def main():

    build_dim_date()
    build_dim_road()
    build_dim_zone()
    build_dim_weather()
    build_fact_traffic()


if __name__ == "__main__":
    main()