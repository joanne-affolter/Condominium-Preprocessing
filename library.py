import numpy as np
#import seaborn as sns
#import matplotlib.pyplot as plt
import pandas as pd 
from datetime import datetime,time


def extract_zipcode_city(x):
    """ 
    Extracts address, zipcode and city from a string.
    """
    try : 
        x = x.split(" ")
        for index, word in enumerate(x) : 
            if word.isdigit() and len(word) == 5 : 
                street = " ".join(x[:index])
                zip = word
                city = " ".join(x[index+1:])
                break

        return street, zip, city
    except : 
        return None, None, None

def extract_SIRET(x) :
    """ 
    Extracts SIRET from a string.
    """ 
    return x[11:25]

def extract_SIREN(x) : 
    """
    Extracts SIREN from a string.
    """
    return x[11:20]

def str2list(x) : 
    """ 
    Converts a string to a list.
    """
    x = x.strip("[]")
    if x is None : 
        return None
    return x.split(",")

def explode_presentation(df) :
    """ 
    Explodes the presentation column of a dataframe.
    """
    df["presentation"] = df["presentation"].apply(str2list)
    df = df.explode("presentation")
    return df

def top_stationnement_per_dep(df, departement, nb_top) :
    """
    Plot top nb_top number of parking lots per syndic in a given department.
    Args : 
        df : merged dataframe
        departement : string
        nb_top : int, number of top syndics to plot 
    Returns : 
        top_df : dataframe of top syndics
    """
    df_dep = df[df["Département"] == departement]
    top_df = df_dep.sort_values(by="Nombre de lots de stationnement", ascending=False)
    top_df = top_df.head(nb_top)[['title', 'Nombre de lots de stationnement']]
    top_df.set_index('title', inplace=True)
    s = top_df['Nombre de lots de stationnement']
    sns.barplot(x=s.values, y=s.index, orient="h")
    #plt.barh(y=s.index, width=s.values)
    plt.show()

    return top_df

def extract_information(x) : 
    """ 
    Extract opening and closing hours from a dict. 
    """
    x = x[1:-1]
    x = x.split(",")
    try :
        lundi, mardi, mercredi, jeudi, vendredi = None, None, None, None, None
        for i in x : 
            i = i.replace("'", "")
            i = i.replace(" ", "")
            day, hours = None, None
            day, hours = i.split(":")[0], i.split(":")[1]
            if day == "Lundi" :
                lundi = hours
            elif day == "Mardi" :
                mardi = hours
            elif day == "Mercredi" :
                mercredi = hours
            elif day == "Jeudi" :
                jeudi = hours
            elif day == "Vendredi" :
                vendredi = hours
        return lundi, mardi, mercredi, jeudi, vendredi
    except :
        return None, None, None, None, None
    
def str2hours(x) :
    """
    Converts a string to a time object.
    """
    open, close = None, None
    if x is None : 
        return open, close
    else : 
        open, close = x.split("-")[0], x.split("-")[1]
        open = time(int(open.split("h")[0]), int(open.split("h")[1]))
        close = time(int(close.split("h")[0]), int(close.split("h")[1]))
    return [open, close]

def filter_df(x, departement, day, start_hour, start_min, end_hour, end_min) : 
    """
    Filter dataframe x by departement, day, and time interval
    """
    x = x[x["Département"] == departement]
    x = x[x[day] != (None, None)]
    x = x[x[day].apply(lambda x : x[0] <= time(start_hour, start_min)  and x[1] >= time(end_hour, end_min))]
    x["Nombre de lots de stationnement"] = x["Nombre de lots de stationnement"].fillna(0)
    x = x.sort_values(by="Nombre de lots de stationnement", ascending=False)

    return x

def get_rdvs(horaires, rdv, df) :
    """ 
    Assign appointments.
    """
    for day, times in horaires.items() : 
        for time in times : 
            start, end = time[0], time[1]
            tmp = filter_df(df, "92", day, start.hour, start.minute, end.hour, end.minute)
            tmp = tmp[tmp["RDV planifié"] == False]
            try : 
                hour_str = f"{start.hour}h{start.minute}:{end.hour}h{end.minute}"
                rdv[day].append((hour_str, tmp.head(1)["title"].item()))
                # Get the index of the first row in 'tmp'
                index_to_update = tmp.head(1).index[0]
                # Update the 'RDV planifié' column in 'df' for the corresponding index
                df.loc[df.ID == index_to_update, "RDV planifié"] = True
            except : 
                rdv[day].append(None)
    return rdv  