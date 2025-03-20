import pandas as pd
import logging
import re

def clean_youtube_data(df):
    logger = logging.getLogger(__name__)

    # Before Cleaning
    logger.info("Starting data cleaning process.")
    
    iso_format = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$'
    
    # Count before cleaning
    timestamp_issues = df[~df['publish_time'].astype(str).str.match(iso_format)].shape[0]
    negative_values = df[(df[['views', 'likes', 'dislikes', 'comment_count']] < 0).any(axis=1)].shape[0]
    leading_trailing_spaces = df[['title', 'channel_title', 'description']].apply(lambda x: x.str.strip()).isnull().sum().sum()
    missing_category_id = df['category_id'].isnull().sum()
    missing_tags = df['tags'].isnull().sum()
    missing_description = df['description'].isnull().sum()
    special_character_tags = df['tags'].str.contains(r'[^a-zA-Z0-9\s]', na=False).sum()
    duplicate_rows = df.duplicated().sum()

    logger.info(f"Before cleaning: Timestamps not in ISO format: {timestamp_issues}, Negative values: {negative_values}, Leading/trailing spaces: {leading_trailing_spaces}, Missing category_id: {missing_category_id}, Missing tags: {missing_tags}, Missing description: {missing_description}, Tags with special characters: {special_character_tags}, Duplicate rows: {duplicate_rows}")

    # Perform Cleaning
    mask = ~df['publish_time'].astype(str).str.match(iso_format)
    df.loc[mask, 'publish_time'] = pd.to_datetime(df.loc[mask, 'publish_time'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    df[['views', 'likes', 'dislikes', 'comment_count']] = df[['views', 'likes', 'dislikes', 'comment_count']].clip(lower=0)

    df[['title', 'channel_title', 'description']] = df[['title', 'channel_title', 'description']].apply(lambda x: x.str.strip())

    df['category_id'].fillna(1000, inplace=True)

    df['tags'] = df['tags'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True).fillna('No Tags')

    df['description'].fillna('', inplace=True)

    df.drop_duplicates(inplace=True)

    # After Cleaning
    logger.info("Data cleaning completed.")
    
    timestamp_issues_fixed = timestamp_issues - (df[~df['publish_time'].astype(str).str.match(iso_format)].shape[0])
    negative_values_fixed = negative_values - df[(df[['views', 'likes', 'dislikes', 'comment_count']] < 0).any(axis=1)].shape[0]
    leading_trailing_spaces_fixed = leading_trailing_spaces - df[['title', 'channel_title', 'description']].isnull().sum().sum()
    missing_category_id_fixed = missing_category_id - df['category_id'].isnull().sum()
    missing_tags_fixed = missing_tags - df['tags'].isnull().sum()
    missing_description_fixed = missing_description - df['description'].isnull().sum()
    special_character_tags_fixed = special_character_tags - df['tags'].str.contains(r'[^a-zA-Z0-9\s]', na=False).sum()
    duplicate_rows_fixed = duplicate_rows - df.duplicated().sum()

    logger.info(f"After cleaning: Fixed timestamps issues: {timestamp_issues_fixed}, Fixed negative values: {negative_values_fixed}, Fixed leading/trailing spaces: {leading_trailing_spaces_fixed}, Fixed missing category_id: {missing_category_id_fixed}, Fixed missing tags: {missing_tags_fixed}, Fixed missing description: {missing_description_fixed}, Fixed special character tags: {special_character_tags_fixed}, Fixed duplicate rows: {duplicate_rows_fixed}")
    
    return df