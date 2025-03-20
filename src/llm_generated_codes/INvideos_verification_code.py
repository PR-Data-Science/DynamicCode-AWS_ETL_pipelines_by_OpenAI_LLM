from pyspark.sql import functions as F

# Check for correct timestamp format
timestamp_check = df.filter(~F.col("publish_time").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")).count()
print("✅ Timestamp format is correct" if timestamp_check == 0 else "❌ Timestamps have incorrect formats")

# Check for non-negative values in numeric columns
views_negative = df.filter(F.col("views").cast("int") < 0).count()
likes_negative = df.filter(F.col("likes").cast("int") < 0).count()
dislikes_negative = df.filter(F.col("dislikes").cast("int") < 0).count()
comment_count_negative = df.filter(F.col("comment_count") < 0).count()

if views_negative == 0 and likes_negative == 0 and dislikes_negative == 0 and comment_count_negative == 0:
    print("✅ No negative values found")
else:
    print("❌ Some numeric values are negative")

# Check for leading/trailing spaces in string columns
title_spaces = df.filter(F.col("title").rlike(r"^\s|\s$")).count()
channel_title_spaces = df.filter(F.col("channel_title").rlike(r"^\s|\s$")).count()
description_spaces = df.filter(F.col("description").rlike(r"^\s|\s$")).count()

if title_spaces == 0 and channel_title_spaces == 0 and description_spaces == 0:
    print("✅ No leading/trailing spaces found in string columns")
else:
    print("❌ Leading/trailing spaces found in some string columns")

# Check for missing tags
missing_tags = df.filter(F.col("tags").isNull() | (F.col("tags") == "")).count()
print("✅ Missing tags replaced" if missing_tags == 0 else "❌ Some missing tags are not replaced")

# Check for missing descriptions
missing_descriptions = df.filter(F.col("description").isNull()).count()
print("✅ Missing descriptions replaced" if missing_descriptions == 0 else "❌ Some missing descriptions are not replaced")