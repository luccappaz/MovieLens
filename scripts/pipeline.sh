#!/usr/bin/bash

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

export PYTHONPATH="$PROJECT_ROOT"

VERDE='\033[0;32m'
VERMELHO='\033[0;31m'
NC='\033[0m' # No color

echo -e "${VERDE} Initiating the Silver Layer pipeline...${NC}"
echo "----------------------------------------"

echo "🎬 Processing 'movies.csv'..."
if ! uv run "$PROJECT_ROOT/transformations/movies.py" > /dev/null 2>&1; then
    echo -e "${VERMELHO}⚠️ WARNING: Critical failure at processing Movies. Aborting...${NC}"
    exit 1
fi

echo "⭐ Processing 'ratings.csv'..."
if ! uv run "$PROJECT_ROOT/transformations/ratings.py" > /dev/null 2>&1; then
    echo -e "${VERMELHO}⚠️ WARNING: Critical failure at processing Ratings. Aborting...${NC}"
    exit 1
fi

echo "🔗 Processing 'links.csv'..."
if ! uv run "$PROJECT_ROOT/transformations/links.py" > /dev/null 2>&1; then
    echo -e "${VERMELHO}⚠️ WARNING: Critical failure at processing Links. Aborting...${NC}"
    exit 1
fi

echo "🏷️ Processing 'tags.csv'..."
if ! uv run "$PROJECT_ROOT/transformations/tags.py" > /dev/null 2>&1; then
    echo -e "${VERMELHO}⚠️ WARNING: Critical failure at processing Tags. Aborting...${NC}"
    exit 1
fi

echo "----------------------------------------"
echo -e "${VERDE}🏁 Silver Layer successfully concluded at MinIO!${NC}"

echo -e "${VERDE} Initiating the Golden Layer pipeline...${NC}"
echo "----------------------------------------"

echo "🎬 Using ALS to get recommendations..."
if ! uv run "$PROJECT_ROOT/transformations/als_recommendations.py" > /dev/null 2>&1; then
    echo -e "${VERMELHO}⚠️ WARNING: Critical failure at processing the recommendations. Aborting...${NC}"
    exit 1
fi

echo "----------------------------------------"
echo -e "${VERDE}🏁 Golden Layer successfully concluded at MinIO!${NC}"
