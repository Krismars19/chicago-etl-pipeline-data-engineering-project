import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(taxi_data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    taxi_data['trip_start_timestamp'] = pd.to_datetime(taxi_data['trip_start_timestamp'])
    taxi_data['trip_end_timestamp'] = pd.to_datetime(taxi_data['trip_end_timestamp'])
    
    datetime_dim = taxi_data[['trip_start_timestamp','trip_end_timestamp']].reset_index(drop=True)

    datetime_dim['trip_start_timestamp'] = datetime_dim['trip_start_timestamp']
    datetime_dim['start_hour'] = datetime_dim['trip_start_timestamp'].dt.hour
    datetime_dim['start_day'] = datetime_dim['trip_start_timestamp'].dt.day
    datetime_dim['start_month'] = datetime_dim['trip_start_timestamp'].dt.month
    datetime_dim['start_year'] = datetime_dim['trip_start_timestamp'].dt.year
    datetime_dim['start_weekday'] = datetime_dim['trip_start_timestamp'].dt.weekday

    datetime_dim['trip_end_timestamp'] = datetime_dim['trip_end_timestamp']
    datetime_dim['end_hour'] = datetime_dim['trip_end_timestamp'].dt.hour
    datetime_dim['end_day'] = datetime_dim['trip_end_timestamp'].dt.day
    datetime_dim['end_month'] = datetime_dim['trip_end_timestamp'].dt.month
    datetime_dim['end_year'] = datetime_dim['trip_end_timestamp'].dt.year
    datetime_dim['end_weekday'] = datetime_dim['trip_end_timestamp'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'trip_start_timestamp','start_hour', 'start_day', 
                             'start_month', 'start_year', 'start_weekday', 'trip_end_timestamp',
                            'end_hour', 'end_day', 'end_month', 'end_year', 'end_weekday']]
    

    trip_distance_dim = taxi_data[['trip_miles']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index

    conversion_factor = 1.60934
    trip_distance_dim['trip_km'] = taxi_data['trip_miles'] * conversion_factor

    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_miles', 'trip_km']]

    trip_hour_dim = taxi_data[['trip_seconds']].reset_index(drop=True)
    trip_hour_dim['trip_hour_id'] = trip_hour_dim.index
    trip_hour_dim['trip_hour'] = taxi_data['trip_seconds'] / 3600
    trip_hour_dim = trip_hour_dim[['trip_hour_id', 'trip_seconds', 'trip_hour']]


    payment_type_id = {
        'Cash':1,
        'Mobile':2, 
        'Credit Card':3, 
        'Unknown':4, 
        'Prcard':5, 
        'No Charge':6,
        'Dispute':7    
    }

    payment_type_dim = taxi_data[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_code'] = payment_type_dim['payment_type'].map(payment_type_id)
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type_code','payment_type']]

    company_ID = {
        'Globe Taxi':1, 
        'Sun Taxi':2, 
        'Chicago Independents':3,
        'Taxi Affiliation Services':4, 
        'Flash Cab':5,
        'Taxicab Insurance Agency, LLC':6, 
        'Choice Taxi Association':7,
        'City Service':8, 
        '24 Seven Taxi':9,
        'Medallion Leasin':10,
        'Patriot Taxi Dba Peace Taxi Associat':11, 
        'Top Cab Affiliation':12,
        'Blue Ribbon Taxi Association Inc.':13, 
        'Setare Inc':14,
        '312 Medallion Management Corp':15, 
        'U Taxicab':16
    }


    company_dim = taxi_data[['company']].reset_index(drop=True)
    company_dim['company_id'] = company_dim.index
    company_dim['company_code'] = company_dim['company'].map(company_ID)
    company_dim['company_name'] = company_dim['company']
    company_dim = company_dim[['company_id', 'company_code', 'company_name']]

    pickup_location_dim = taxi_data[['pickup_centroid_longitude', 'pickup_centroid_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_centroid_latitude','pickup_centroid_longitude']] 

    dropoff_location_dim = taxi_data[['dropoff_centroid_longitude', 'dropoff_centroid_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_centroid_latitude','dropoff_centroid_longitude']]    

    dropoff_comunity_area_dim = taxi_data[['dropoff_communityID', 'dropoff_community_area']].reset_index(drop=True)
    dropoff_comunity_area_dim['dropoff_community_area_id'] = dropoff_comunity_area_dim.index
    dropoff_comunity_area_dim = dropoff_comunity_area_dim[['dropoff_community_area_id','dropoff_communityID', 'dropoff_community_area']]

    pickup_comunity_area_dim = taxi_data[['pickup_communityID', 'pickup_community_area']].reset_index(drop=True)
    pickup_comunity_area_dim['pickup_community_area_id'] = pickup_comunity_area_dim.index
    pickup_comunity_area_dim = pickup_comunity_area_dim[['pickup_community_area_id','pickup_communityID', 'pickup_community_area']]

    taxi_trip_fact = taxi_data.merge(datetime_dim, left_on='taxi_trip_id', right_on='datetime_id') \
            .merge(trip_distance_dim, left_on='taxi_trip_id', right_on='trip_distance_id') \
            .merge(payment_type_dim, left_on='taxi_trip_id', right_on='payment_type_id') \
            .merge(trip_hour_dim, left_on='taxi_trip_id', right_on='trip_hour_id') \
            .merge(company_dim, left_on='taxi_trip_id', right_on='company_id') \
            .merge(dropoff_location_dim, left_on='taxi_trip_id', right_on='dropoff_location_id') \
            .merge(pickup_location_dim, left_on='taxi_trip_id', right_on='pickup_location_id') \
            .merge(dropoff_comunity_area_dim, left_on='taxi_trip_id', right_on='dropoff_community_area_id') \
            .merge(pickup_comunity_area_dim, left_on='taxi_trip_id', right_on='pickup_community_area_id') \
            [['taxi_trip_id', 'trip_id', 'taxi_id', 'datetime_id', 'trip_distance_id', 'payment_type_id',
            'company_id', 'dropoff_community_area_id', 'dropoff_location_id', 'pickup_community_area_id',
            'pickup_location_id', 'trip_hour_id', 'fare', 'tips', 'tolls', 'extras', 'trip_total']]

    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "trip_distance_dim":trip_distance_dim.to_dict(orient="dict"),
    "trip_hour_dim":trip_hour_dim.to_dict(orient="dict"),
    "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
    "company_dim":company_dim.to_dict(orient="dict"),
    "pickup_location_dim":pickup_location_dim.to_dict(orient="dict"),
    "dropoff_location_dim":dropoff_location_dim.to_dict(orient="dict"),
    "dropoff_comunity_area_dim":dropoff_comunity_area_dim.to_dict(orient="dict"),
    "pickup_comunity_area_dim":pickup_comunity_area_dim.to_dict(orient="dict"),
    "taxi_trip_fact":taxi_trip_fact.to_dict(orient="dict")}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
