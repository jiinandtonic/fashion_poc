---
title: Fashion Trend Recommender (PoC)
---

# Fashion Trend Recommender (PoC)

**What it is:** A personal menswear trend recommender. It analyzes images to detect styles
(e.g., streetwear, casual, formal, vintage) and recommends items that fit the user’s look.

**What it does with Pinterest/Reddit:** Read-only access to images and metadata from boards
(or subreddits) I own or explicitly authorize. No posting.

**How it works (high level):**
- Ingest curated images (Pinterest boards or subreddit image posts)
- Compute visual embeddings (CLIP) + style tags
- Track trend velocity over time
- Personalize to the user (optional) via similarity to their reference photo

**Contact:** rongjiinc@gmail.com

**Privacy:** See the [Privacy Policy](privacy.md).
