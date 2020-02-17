import boto3

def get_object_list (city, prefix, bucket):
    """
    returns a list of objects residing in the S3 bucket for a given city and file name prefix
    ...

    Parameters:
    -----------
    city : str
        name of the city
    prefix : str
        file name prefix either calendar.csv or listings.csv
    bucket : str
        name of S3 bucket

    Returns:
    --------
    objs : list
        list containing keys to S3 objects
    """
    objs = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    for object in bucket.objects.all():
       if object.key.startswith(city) and object.key.endswith(prefix):
            objs.append(object.key)
       else:
            continue
    return(objs)


def get_city_list(bucket):
    """
    returns a list of cities corresponds to the objects in the S3 bucket
    ...

    Parameters:
    -----------
    bucket : str
        name of S3 bucket

    Returns:
    --------
    cities : list
        list containing all the city names attributed to the S3 bucket objects
    """
    cities = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    for object in bucket.objects.all():
        city = object.key.split('_')[0]
        if city not in cities:
            cities.append(city)
    return(cities)
