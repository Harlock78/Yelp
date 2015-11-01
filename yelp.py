import rauth,sys, urllib, re,pdb
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests


def authenticate():
  """Return session object required to request a search"""

  consumer_key = 'Jjw5GZkJ8rLbcrKmGW5RwA'
  consumer_secret = 'uVgw3EhnfLEABve48wA4UmkbJp4'
  token = 'B2nfmgjMT6a8d5XYck6bScczxW1gRxdt'
  token_secret = '_1n06oKU5smCTns1YWBPZjjit3E'

  session = rauth.OAuth1Session(
    consumer_key = consumer_key
    ,consumer_secret = consumer_secret
    ,access_token = token
    ,access_token_secret = token_secret)
  return session

def parameters(category,place):
  params={}
  params['term']='restaurant'
  params['radius_filter']='2000'
  params['location']=place
  params['category_filter']=category
  return params

def get_cvs_files():
  """ Extract all names from places availables in the ACS survey, with their population census (2010)"""
  urllib.urlretrieve("http://www.census.gov/popest/data/cities/totals/2012/files/SUB-EST2012.csv",'places_ACS.csv')
  df_places = pd.read_csv('places_ACS.csv')

  """Scrap the list of possible restaurant categories from yelp help page"""
  r = requests.get('https://www.yelp.com/developers/documentation/v2/all_category_list')
  html = r.text
  soup = BeautifulSoup(html)
  chunk=(soup.text.split('Restaurants (restaurants, All)')[1]).split('Shopping')[0]
  categories = re.findall(r"\((\w+),",chunk)   
  return df_places, categories

def get_place_code(place):
  """get ANSI codes"""
  st_code = STATE_CODE[place.split(',')[1]][0]
  st_abr = STATE_CODE[place.split(',')[1]][1]
  url = 'http://www2.census.gov/geo/docs/reference/codes/files/st'+st_code+'_'+st_abr+'_places.txt'
  r = requests.get(url)
  text=r.text
  code_list = text.split('\n')
  pl_name = place.split(',')[0]
  for item in code_list:
    if pl_name in item:
      code_place = item.split('|')[2]
      break
  return code_place,st_code

def get_ACS_census_data(state_code,place_code):
  url = 'http://quickfacts.census.gov/qfd/states/'+state_code+'/'+state_code+place_code+'.html'
  try:
    dfs = pd.read_html(url)
  except:
    return
  ndf = dfs[2]
  place_data_city = {}
  for i in xrange(len(ndf)):
    place_data_city[ndf[1].iloc[i]]=ndf[2].iloc[i]
  place_data_city.pop(place_data_city.keys()[0])
  return place_data_city


def exploration(df):
  """looking at mexican restaurant vs hispanic population"""
  select1 = df[['Hispanic or Latino, percent, 2010 (b)','Population, 2010','city','state_code','name','categories','review_count','rating']]  
  select1['Population, 2010'] = select1['Population, 2010'].astype(int)
  mex=select1[select1['categories'].str.contains('mexican')]
  mex['review_count'] = mex['review_count'].astype(int)
  mex['rating'] = mex['rating'].astype(float)
  num = []
  for i in xrange(len(m)):
    num.append(float((mex['Hispanic or Latino, percent, 2010 (b)'].iloc[i]).split('%')[0])/100)
  mex['Hispanic or Latino, percent, 2010 (b)']=num
  grps = mex.groupby('city')  #group by cities
  dfs = []
  for city in pd.unique(mex['city']):
    dfs.append(grps.get_group(city)) 
  for frame in dfs:
    stats.append((frame[frame.columns[0]].iloc[0],frame['Population, 2010'].iloc[0],frame['review_count'].describe(),frame['rating'].describe()))  
  ratings = []; reviews = []; percent_hisp = []; pop = []; metrics = []; n_competitors = [];spread_rating = []
  for item in stats:
    ratings.append(item[3]['mean'])
    reviews.append(item[2]['mean'])
    percent_hisp.append(item[0])
    pop.append(int(item[1]))
    n_competitors.append(item[2]['count'])
    spread_rating.append((item[2]['max']-item[2]['50%']))
  for i in xrange(len(ratings)):
    metrics.append(reviews[i]*ratings[i]/pop[i])
    spread_rating[i]=spread_rating[i]/pop[i]
  plots=pd.DataFrame(columns=['hisp','pop','n_competitors','ratings','reviews','spread_rating','metrics'])
  plots['hisp']=percent_hisp
  plots['pop']=pop
  plots['ratings']=ratings
  plots['reviews']=reviews
  plots['n_competitors']=n_competitors
  plots['spread_rating']=spread_rating
  plots['metrics']=metrics
  return plots

def build_database():
  STATE_CODE={'Maryland':('24','md'),'Virginia':('51','va'),'California':('06','ca'),
           'Arizona':('4','az'),'Washington':('53','wa'),'Massachusetts':('25','ma')}

  session = authenticate()

  df_places, categories = get_cvs_files()
  """for now only select 50 most populous places in maryland and virginia"""
  df_VA = df_places[['NAME','STNAME','CENSUS2010POP']][(df_places['STNAME']=='Virginia')].drop_duplicates(cols=['NAME','STNAME','CENSUS2010POP'])
  df_VA = df_VA[(df_VA['NAME'].str.contains('city')) | (df_VA['NAME'].str.contains('town'))]
  df_VA['CENSUS2010POP'] = df_VA['CENSUS2010POP'].replace(['X'],['0'])
  df_VA['CENSUS2010POP']=df_VA['CENSUS2010POP'].astype(int)
  df_VA = df_VA[(df_VA['CENSUS2010POP']>5000)]
  df_VA=df_VA.sort(['CENSUS2010POP'],ascending=False)
  df_VA = df_VA.iloc[1:51]

  df_MD = df_places[['NAME','STNAME','CENSUS2010POP']][(df_places['STNAME']=='Maryland')].drop_duplicates(cols=['NAME','STNAME','CENSUS2010POP'])
  df_MD = df_MD[(df_MD['NAME'].str.contains('city')) | (df_MD['NAME'].str.contains('town'))]
  df_MD['CENSUS2010POP'] = df_MD['CENSUS2010POP'].replace(['X'],['0'])
  df_MD['CENSUS2010POP']=df_MD['CENSUS2010POP'].astype(int)
  df_MD = df_MD[(df_MD['CENSUS2010POP']>5000)]
  df_MD=df_MD.sort(['CENSUS2010POP'],ascending=False)
  df_MD = df_MD.iloc[1:51]
  df_both = df_MD.append(df_VA)  

  df_CA = df_places[['NAME','STNAME','CENSUS2010POP']][(df_places['STNAME']=='California')].drop_duplicates(cols=['NAME','STNAME','CENSUS2010POP'])
  df_CA = df_CA[(df_CA['NAME'].str.contains('city')) | (df_CA['NAME'].str.contains('town'))]
  df_CA['CENSUS2010POP'] = df_CA['CENSUS2010POP'].replace(['X'],['0'])
  df_CA['CENSUS2010POP']=df_CA['CENSUS2010POP'].astype(int)
  df_CA = df_CA[(df_CA['CENSUS2010POP']>5000)]
  df_CA=df_CA.sort(['CENSUS2010POP'],ascending=False)
  df_CA = df_CA.iloc[1:51]

  df_tri = df_both.append(df_CA)

  place_list = df_tri['NAME']+','+df_tri['STNAME']+',USA'
  category_list = categories

  code_place,st_code = get_place_code('Frederick city,Maryland,USA')
  #code_place,st_code = get_place_code('Seattle city,Washington,USA')
  place_data_city = get_ACS_census_data(st_code,code_place)

  columns = ['latitude','longitude'] + ['is_claimed','rating','review_count','name','url',
            'is_closed','snippet_text','categories','city',
            'state_code','postal_code','country_code',
            'geo_accuracy','address']
  columns = columns + place_data_city.keys()
  df = pd.DataFrame(columns=columns)

  row = -1
  for place in place_list:
    try:
      code_place,st_code = get_place_code(place)
      place_data_city = get_ACS_census_data(st_code,code_place)
    except:
      continue

    for category in category_list:
      params = parameters(category,place)

      request = session.get('https://api.yelp.com/v2/search',params=params)
      if 'error' not in request.text:
        data = request.json()
      else:
        pdb.set_trace()
        continue

      if (len(data['businesses'])>0) and (place_data_city is not None):
        #fill dataframe
        for i in xrange(len(data['businesses'])):
          row += 1
          df.loc[row]=['nan' for j in xrange(len(columns))]
          df.loc[row][place_data_city.keys()]=place_data_city.values()
          try:
            df.loc[row]['latitude']=data['region']['center'].values()[0]
          except:
            df.loc[row]['latitude']='nan'
          try:
            df.loc[row]['longitude']=data['region']['center'].values()[1]
          except:
            df.loc[row]['longitude']='nan'
          try:
            df.loc[row]['is_claimed']=data['businesses'][i]['is_claimed']
          except:
            df.loc[row]['is_claimed']='nan'
          try:
            df.loc[row]['rating']=data['businesses'][i]['rating']
          except:
            df.loc[row]['rating']='nan'
          try:
            df.loc[row]['review_count']=data['businesses'][i]['review_count']
          except:
            df.loc[row]['review_count']='nan'
          try:
            df.loc[row]['name']=data['businesses'][i]['name']
          except:
            df.loc[row]['name']='nan'
          try:
            df.loc[row]['url']=data['businesses'][i]['url']
          except:
            df.loc[row]['url']='nan'
          try:
            df.loc[row]['is_closed']=data['businesses'][i]['is_closed']
          except:
            df.loc[row]['is_closed']='nan'
          try:
            df.loc[row]['snippet_text']=data['businesses'][i]['snippet_text']
          except:
            df.loc[row]['snippet_text']='nan'
          try:
            df.loc[row]['categories']=data['businesses'][i]['categories']
          except:
            df.loc[row]['categories']='nan'
          try:
            df.loc[row]['categories']=str(data['businesses'][i]['categories'])
          except:
            df.loc[row]['categories']='nan'
          try:
            df.loc[row]['city']=data['businesses'][i]['location']['city']
          except:
            df.loc[row]['city']='nan'
          try:
            df.loc[row]['state_code']=data['businesses'][i]['location']['state_code']
          except:
            df.loc[row]['state_code']='nan'
          try:
            df.loc[row]['postal_code']=data['businesses'][i]['location']['postal_code']
          except:
            df.loc[row]['postal_code']='nan'
          try:
            df.loc[row]['country_code']=data['businesses'][i]['location']['country_code']
          except:
            df.loc[row]['country_code']='nan'
          try:
            df.loc[row]['geo_accuracy']=data['businesses'][i]['location']['geo_accuracy']
          except:
            df.loc[row]['geo_accuracy']='nan'
          try:
            df.loc[row]['address']=str(data['businesses'][i]['address'])
          except:
            df.loc[row]['address']='nan'
  return df



