import pandas as pd
import unicodedata


netflix = pd.read_csv("C:\\Users\\dilek\\Desktop\\Netflix\\netflix_titles.csv")
basics = pd.read_csv("C:\\Users\\dilek\\Desktop\\Netflix\\title.basics.tsv", sep="\t", na_values='\\N', low_memory=False)
ratings = pd.read_csv("C:\\Users\\dilek\\Desktop\\Netflix\\title.ratings.tsv", sep="\t")


netflix_movies = netflix[netflix['type'] == 'Movie'].copy()
netflix_movies = netflix_movies[['title', 'release_year']]


def normalize_title(title):
    return unicodedata.normalize('NFKD', str(title)).encode('ascii', 'ignore').decode('utf-8').lower().strip()

netflix_movies['title_clean'] = netflix_movies['title'].apply(normalize_title)

imdb_movies = basics[basics['titleType'] == 'movie'][['tconst', 'primaryTitle', 'startYear']]
imdb_movies = imdb_movies.dropna()
imdb_movies['title_clean'] = imdb_movies['primaryTitle'].apply(normalize_title)
imdb_movies['startYear'] = imdb_movies['startYear'].astype(int)


imdb_full = pd.merge(imdb_movies, ratings, on='tconst')


merged = pd.merge(netflix_movies, imdb_full,
                  left_on=['title_clean', 'release_year'],
                  right_on=['title_clean', 'startYear'],
                  how='inner')

print(f"Number of raw records matched: {len(merged)}")


duplicate_titles = merged.groupby(['title_clean', 'release_year']).size()
duplicate_titles = duplicate_titles[duplicate_titles > 1]
print(f"Number of repeated movies (name + year): {len(duplicate_titles)}")


merged_sorted = merged.sort_values(by='numVotes', ascending=False)
merged_deduped = merged_sorted.drop_duplicates(subset=['title_clean', 'release_year'], keep='first')
print(f"Number of matches cleared: {len(merged_deduped)}")


df = merged_deduped[['title', 'release_year', 'averageRating', 'numVotes']].rename(columns={
    'release_year': 'year',
    'averageRating': 'imdb_score',
    'numVotes': 'imdb_votes'
})

df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
df['imdb_score'] = pd.to_numeric(df['imdb_score'], errors='coerce').astype('float')
df['imdb_votes'] = pd.to_numeric(df['imdb_votes'], errors='coerce').astype('Int64')


df = df.drop_duplicates()
df = df.dropna()

print(f"Number of records that can be analyzed in their final form: {len(df)}")


df.to_csv("C:\\Users\\dilek\\Desktop\\Netflix\\netflix_imdb_merged.csv", index=False)
print("✅ Clean data was saved successfully as 'netflix_imdb_merged.csv'")
