import pandas as pd
import logging
import re

def clean_youtube_data(df):
    logger = logging.getLogger(__name__)

    # Before Cleaning
    iso_format = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$'
    
    logger.info("Counting issues before cleaning...")
    iso_format_count = df[~df['publish_time'].str.match(iso_format)].shape[0]
    negative_views_count = df[df['views'] < 0].shape[0]
    negative_likes_count = df[df['likes'] < 0].shape[0]
    negative_dislikes_count = df[df['dislikes'] < 0].shape[0]
    negative_comments_count = df[df['comment_count'] < 0].shape[0]
    leading_trailing_space_title_count = df['title'].str.contains(r'^\s|\s$', regex=True).sum()
    leading_trailing_space_channel_count = df['channel_title'].str.contains(r'^\s|\s$', regex=True).sum()
    leading_trailing_space_description_count = df['description'].str.contains(r'^\s|\s$', regex=True).sum()
    missing_category_id_count = df['category_id'].isnull().sum() + df['category_id'].isna().sum()
    missing_tags_count = df['tags'].isnull().sum() + df['tags'].isna().sum()
    missing_description_count = df['description'].isnull().sum() + df['description'].isna().sum()
    special_char_tags_count = df['tags'].str.contains(r'[^a-zA-Z0-9\s]', regex=True).sum()
    duplicate_rows_count = df.duplicated().sum()

    logger.info(f"Before cleaning - ISO format issues: {iso_format_count}, Negative views: {negative_views_count}, "
                f"Negative likes: {negative_likes_count}, Negative dislikes: {negative_dislikes_count}, "
                f"Negative comments: {negative_comments_count}, Leading/trailing space title: {leading_trailing_space_title_count}, "
                f"Leading/trailing space channel: {leading_trailing_space_channel_count}, "
                f"Leading/trailing space description: {leading_trailing_space_description_count}, "
                f"Missing category id: {missing_category_id_count}, Missing tags: {missing_tags_count}, "
                f"Missing description: {missing_description_count}, Special character tags: {special_char_tags_count}, "
                f"Duplicate rows: {duplicate_rows_count}")

    # Cleaning Steps
    # Step 1: Convert publish_time to ISO 8601 format

    # Define the correct ISO 8601 format pattern
    iso_format = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$'
    # Convert only if the format is incorrect
    mask = ~df['publish_time'].astype(str).str.match(iso_format)
    # Convert only incorrectly formatted timestamps
    df.loc[mask, 'publish_time'] = pd.to_datetime(df.loc[mask, 'publish_time'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
   
    # Step 2: Ensure non-negative values
    df.loc[df['views'] < 0, 'views'] = 0
    df.loc[df['likes'] < 0, 'likes'] = 0
    df.loc[df['dislikes'] < 0, 'dislikes'] = 0
    df.loc[df['comment_count'] < 0, 'comment_count'] = 0

    # Step 3: Trim string fields
    df['title'] = df['title'].str.strip()
    df['channel_title'] = df['channel_title'].str.strip()
    df['description'] = df['description'].str.strip()

    # Step 4: Fill missing category_id
    df['category_id'].fillna(1000, inplace=True)

    # Step 5: Remove special characters from tags and fill missing tags
    df['tags'] = df['tags'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
    df['tags'].fillna('No Tags', inplace=True)

    # Step 6: Fill missing descriptions
    df['description'].fillna('', inplace=True)

    # Step 7: Remove duplicates
    df.drop_duplicates(inplace=True)

    # After Cleaning
    logger.info("Counting issues after cleaning...")
    iso_format_count_after = df[~df['publish_time'].str.match(iso_format)].shape[0]
    negative_views_count_after = df[df['views'] < 0].shape[0]
    negative_likes_count_after = df[df['likes'] < 0].shape[0]
    negative_dislikes_count_after = df[df['dislikes'] < 0].shape[0]
    negative_comments_count_after = df[df['comment_count'] < 0].shape[0]
    leading_trailing_space_title_count_after = df['title'].str.contains(r'^\s|\s$', regex=True).sum()
    leading_trailing_space_channel_count_after = df['channel_title'].str.contains(r'^\s|\s$', regex=True).sum()
    leading_trailing_space_description_count_after = df['description'].str.contains(r'^\s|\s$', regex=True).sum()
    missing_category_id_count_after = df['category_id'].isnull().sum()
    missing_tags_count_after = df['tags'].isnull().sum()
    missing_description_count_after = df['description'].isnull().sum()
    special_char_tags_count_after = df['tags'].str.contains(r'[^a-zA-Z0-9\s]', regex=True).sum()
    duplicate_rows_count_after = df.duplicated().sum()

    logger.info(f"After cleaning - ISO format issues: {iso_format_count_after}, Negative views: {negative_views_count_after}, "
                f"Negative likes: {negative_likes_count_after}, Negative dislikes: {negative_dislikes_count_after}, "
                f"Negative comments: {negative_comments_count_after}, Leading/trailing space title: {leading_trailing_space_title_count_after}, "
                f"Leading/trailing space channel: {leading_trailing_space_channel_count_after}, "
                f"Leading/trailing space description: {leading_trailing_space_description_count_after}, "
                f"Missing category id: {missing_category_id_count_after}, Missing tags: {missing_tags_count_after}, "
                f"Missing description: {missing_description_count_after}, Special character tags: {special_char_tags_count_after}, "
                f"Duplicate rows: {duplicate_rows_count_after}")

    # Log warnings if any issues still remain
    if iso_format_count_after > 0:
        logger.warning(f"ISO format issues remain: {iso_format_count_after}")
    if negative_views_count_after > 0:
        logger.warning(f"Negative views remain: {negative_views_count_after}")
    if negative_likes_count_after > 0:
        logger.warning(f"Negative likes remain: {negative_likes_count_after}")
    if negative_dislikes_count_after > 0:
        logger.warning(f"Negative dislikes remain: {negative_dislikes_count_after}")
    if negative_comments_count_after > 0:
        logger.warning(f"Negative comments remain: {negative_comments_count_after}")
    if leading_trailing_space_title_count_after > 0:
        logger.warning(f"Leading/trailing spaces in title remain: {leading_trailing_space_title_count_after}")
    if leading_trailing_space_channel_count_after > 0:
        logger.warning(f"Leading/trailing spaces in channel title remain: {leading_trailing_space_channel_count_after}")
    if leading_trailing_space_description_count_after > 0:
        logger.warning(f"Leading/trailing spaces in description remain: {leading_trailing_space_description_count_after}")
    if missing_category_id_count_after > 0:
        logger.warning(f"Missing category ids remain: {missing_category_id_count_after}")
    if missing_tags_count_after > 0:
        logger.warning(f"Missing tags remain: {missing_tags_count_after}")
    if missing_description_count_after > 0:
        logger.warning(f"Missing descriptions remain: {missing_description_count_after}")
    if special_char_tags_count_after > 0:
        logger.warning(f"Special characters in tags remain: {special_char_tags_count_after}")
    if duplicate_rows_count_after > 0:
        logger.warning(f"Duplicate rows remain: {duplicate_rows_count_after}")

    return df