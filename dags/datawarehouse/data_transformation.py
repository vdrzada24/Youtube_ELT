from datetime import timedelta,datetime
def transform_duration(duration):
    
    duration = duration.replace("P","").replace("T","")

    components = ["D","H","M","S"]
    values = {"D":0,"H":0,"M":0,"S":0}

    for component in components:
        if component in duration:
            value , duration = duration.split(component)
            values[component] = int(value)

    total_duration = timedelta(days = values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"])

    return total_duration

def transform_data(row):

    t_duration = transform_duration(row["Duration"])

    row["Duration"] = (datetime.min + t_duration).time()

    return row