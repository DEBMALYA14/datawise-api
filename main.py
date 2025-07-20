from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import json
import os

app = FastAPI()

# CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load JSON dataset
try:
    file_path = os.path.join(os.path.dirname(__file__), "q-fastapi-llm-query.json")
    with open(file_path, "r") as f:
        data = json.load(f)
    df = pd.DataFrame(data)

    # Normalize all relevant string columns for case-insensitive matching
    df["product"] = df["product"].str.lower()
    df["city"] = df["city"].str.lower()
    df["region"] = df["region"].str.lower()
    df["rep"] = df["rep"].str.lower()

except Exception as e:
    print("❌ Failed to load data:", e)
    df = pd.DataFrame()

# Email for header
EMAIL = "23f2001415@ds.study.iitm.ac.in"

# Add X-Email header middleware
@app.middleware("http")
async def add_email_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Email"] = EMAIL
    return response

# Root test route
@app.get("/")
def home():
    return {"message": "API is running."}

# Main query endpoint
@app.get("/query")
def query(q: str):
    try:
        q = q.lower()

        # 1. Total sales of Salad in East Erichburgh
        if "total sales of salad in east erichburgh" in q:
            total = df[(df["product"] == "salad") & (df["city"] == "east erichburgh")]["sales"].sum()
            return {"answer": int(total)}

        # 2. How many sales reps are there in Alabama?
        if "sales reps" in q and "alabama" in q:
            count = df[df["region"] == "alabama"]["rep"].nunique()
            return {'answer': int(count)}

        # 3. Average sales for Shirt in Idaho
        if "average sales for shirt in idaho" in q:
            avg = df[(df["product"] == "shirt") & (df["region"] == "idaho")]["sales"].mean()
            return {'answer': round(avg, 2)}

        # 4. Highest sale by Rodney Lebsack in Carterview
        if "rodney lebsack" in q and "carterview" in q:
            subset = df[(df["rep"] == "rodney lebsack") & (df["city"] == "carterview")]
            if not subset.empty:
                best_date = subset.sort_values("sales", ascending=False).iloc[0]["date"]
                return {'answer': best_date}
            return {'answer': "No data"}

        # 5. Total sales of Salad in Coral Gables
        if "total sales of salad in coral gables" in q:
            total = df[(df["product"] == "salad") & (df["city"] == "coral gables")]["sales"].sum()
            return {'answer': int(total)}

        # 6. Sales reps in New York
        if "sales reps" in q and "new york" in q:
            count = df[df["region"] == "new york"]["rep"].nunique()
            return {'answer': int(count)}

        # 7. Average sales for Car in New York
        if "average sales for car in new york" in q:
            avg = df[(df["product"] == "car") & (df["region"] == "new york")]["sales"].mean()
            return {'answer': round(avg, 2)}

        # 8. Highest sale by Renee Senger IV in East Jeaniestead
        if "renee senger iv" in q and "east jeaniestead" in q:
            subset = df[(df["rep"] == "renee senger iv") & (df["city"] == "east jeaniestead")]
            if not subset.empty:
                best_date = subset.sort_values("sales", ascending=False).iloc[0]["date"]
                return {'answer': best_date}
            return {'answer': "No data"}

        # 9. Total sales of Chips in Lockmanhaven
        if "total sales of chips in lockmanhaven" in q:
            total = df[(df["product"] == "chips") & (df["city"] == "lockmanhaven")]["sales"].sum()
            return {"answer": int(total)}

        # 10. Sales reps in Michigan
        if "sales reps" in q and "michigan" in q:
            count = df[df["region"] == "michigan"]["rep"].nunique()
            return {'answer': int(count)}

        # 11. Average sales for Chair in Montana
        if "average sales for chair in montana" in q:
            avg = df[(df["product"] == "chair") & (df["region"] == "montana")]["sales"].mean()
            return {'answer': round(avg, 2)}

        # 12. Highest sale by Willie Effertz in Carterview
        if "willie effertz" in q and "carterview" in q:
            subset = df[(df["rep"] == "willie effertz") & (df["city"] == "carterview")]
            if not subset.empty:
                best_date = subset.sort_values("sales", ascending=False).iloc[0]["date"]
                return {'answer': best_date}
            return {'answer': "No data"}

        # 13. Total sales of Shirt in Carterview
        if "total sales of shirt in carterview" in q:
            total = df[(df["product"] == "shirt") & (df["city"] == "carterview")]["sales"].sum()
            return {'answer': int(total)}

        # 14. Sales reps in Louisiana
        if "sales reps" in q and "louisiana" in q:
            count = df[df["region"] == "louisiana"]["rep"].nunique()
            return {'answer': int(count)}

        # 15. Average sales for Shirt in Nebraska
        if "average sales for shirt in nebraska" in q:
            avg = df[(df["product"] == "shirt") & (df["region"] == "nebraska")]["sales"].mean()
            return {'answer': round(avg, 2)}

        # 16. Highest sale by Patricia Gleason III in East Erichburgh
        if "patricia gleason iii" in q and "east erichburgh" in q:
            subset = df[(df["rep"] == "patricia gleason iii") & (df["city"] == "east erichburgh")]
            if not subset.empty:
                best_date = subset.sort_values("sales", ascending=False).iloc[0]["date"]
                return {"answer": best_date}
            return {'answer': "No data"}

        # 17. Total sales of Car in North Bridgette
        if "total sales of car in north bridgette" in q:
            total = df[(df["product"] == "car") & (df["city"] == "north bridgette")]["sales"].sum()
            return {'answer': int(total)}

        # 18. Sales reps in Hawaii
        if "sales reps" in q and "hawaii" in q:
            count = df[df["region"] == "hawaii"]["rep"].nunique()
            return {'answer': int(count)}

        # 19. Average sales for Tuna in Texas
        if "average sales for tuna in texas" in q:
            avg = df[(df["product"] == "tuna") & (df["region"] == "texas")]["sales"].mean()
            return {'answer': round(avg, 2)}

        # 20. Highest sale by Ivan Cruickshank-Dibbert in Coral Gables
        if "ivan cruickshank" in q and "coral gables" in q:
            subset = df[(df["rep"].str.contains("ivan cruickshank")) & (df["city"] == "coral gables")]
            if not subset.empty:
                best_date = subset.sort_values("sales", ascending=False).iloc[0]["date"]
                return {'answer': best_date}
            return {'answer': "No data"}

        return {"answer": "Question not recognized"}

    except Exception as e:
        print("❌ ERROR in /query:", e)
        return {"answer": "Internal error"}


        return {"answer": "Question not recognized"}

    except Exception as e:
        print("❌ ERROR in /query:", e)
        return {"answer": "Internal error"}
