# %%

import http.client
import json
import time
import sys
import collections

# some libraries needed. Note that they are all Python Standard Library
import math
import os


# start = time.time()

api_key = sys.argv[1]

total_movies = 350

# Default is 20 movies per page, but one movie might have <20 similar movies)
movie_per_page = 20

# To collect 350 movies, we need 18 pages in total, but only need first 10 in page 18.
pages_needed = math.ceil(total_movies / movie_per_page)

conn = http.client.HTTPSConnection("api.themoviedb.org")
payload = "{}"

# %% for loop to retrieve 350 movies and write in csv file
try:
    os.remove("movie_ID_name.csv")
except OSError:
    pass

f = open('movie_ID_name.csv', 'a+')
movie_list = []


# request_count = 0
for page in range(1, pages_needed + 1, 1):
    conn.request("GET", "/3/discover/movie?with_genres=18&primary_release_date.gte=2004-01-01&page=" +str(page) + "&include_video=false&include_adult=false&sort_by=popularity.desc&language=en-US&api_key=" + api_key, payload)
    res = conn.getresponse()
    data = res.read()
    movie_db = data.decode("utf-8")
    movie_db = json.loads(movie_db)
    if page == 18:
        movie_db['results'] = movie_db['results'][:10]
    for movie in movie_db['results']:
        movie_info_str = str(movie['id']) + ',' + movie['original_title']
        movie_list.append(movie['id'])
        f.write(movie_info_str + '\n')
    # print('Page', page, 'retreived...')

f.close()

# %% Retreive similar movies for each movie in the above document
no_similar_movie = 5
request_count = 18  # Because 18 requests already in the above part
sim_list_a = []

for movie_id in movie_list:
    if request_count == 40:
        time.sleep(10)
        request_count = 0
    conn.request("GET", "/3/movie/" + str(movie_id) +
                 "/similar?page=1&language=en-US&api_key=" + api_key, payload)
    request_count += 1
    res = conn.getresponse()
    data = res.read()
    movie_db = data.decode("utf-8")
    movie_db = json.loads(movie_db)
    # if this movie doesn't have any similar movies
    if not(movie_db['results']):
        continue  # move on to next movie_id

    if len(movie_db['results']) > no_similar_movie:
        movie_db['results'] = movie_db['results'][:no_similar_movie]

    for movie in movie_db['results']:
        sim_list_a.append([movie_id, movie['id']])
        
    # print('Movie_id', movie_id, 'retreived...')

# %% find same pairs in the list 
# lista = []
# for pair in sim_list_a:
#     a = pair[0]
#     b = pair[1]
#     for pairs in sim_list_a:
#         if a == pairs[1] and b == pairs[0]:
#             lista.append(pair)
# print(len(lista)/2)

# %% Create a new similar movie list containing none-repeated movie and similar-movie pairs
sim_list_b = []
for pairs in sim_list_a:
    a = pairs[0]
    b = pairs[1]
    sim_list_b.append([a, b])
    if [b, a] in sim_list_b:
        if a < b:
            sim_list_b.remove([b, a])
        else:
            sim_list_b.remove([a, b])
            continue
            
# print('sim_list_a len:', len(sim_list_a))
# print('sim_list_b len:', len(sim_list_b))

# %% Write movie, similar movie pairs into csv
try:
    os.remove("movie_ID_sim_movie_ID.csv")
except OSError:
    pass

f = open('movie_ID_sim_movie_ID.csv', 'a+')

for pairs in sim_list_b:
    sim_movie_str = str(pairs[0]) + ',' + str(pairs[1])
    f.write(sim_movie_str + '\n')

f.close()


# end = time.time()
# print(end - start)

