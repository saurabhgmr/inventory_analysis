from flask import Flask, request, jsonify
from flask_cors import CORS  # Import flask-cors
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import os

# Initialize Flask app
app = Flask(__name__)

# Enable CORS for all routes
CORS(app)

# Environment variables for database credentials
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "replicator")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Ant-admin@123")
DB_HOST = os.getenv("DB_HOST", "34.100.200.180")
DB_PORT = int(os.getenv("DB_PORT", 5432))

# Function to connect to the database
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print("Database connection error:", e)
        return None

# Route to fetch all material IDs
@app.route("/materials", methods=["GET"])
def get_all_materials():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Unable to connect to the database"}), 500

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query to fetch distinct material IDs and descriptions
        query = "SELECT DISTINCT material_id, material_description FROM material_data"
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            return jsonify({"error": "No materials found"}), 404
        
        materials = [
            {"material_id": row["material_id"], "material_description": row["material_description"]}
            for row in results
        ]
        return jsonify({"materials": materials}), 200
    except Exception as e:
        print("Error while fetching materials:", e)
        return jsonify({"error": "An error occurred while fetching materials"}), 500
    finally:
        cursor.close()
        conn.close()


# Route to fetch data for a selected material ID
@app.route("/materials/data", methods=["GET"])
def get_material_data():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Unable to connect to the database"}), 500

    try:
        material_ids = request.args.get("material_ids")
        
        if not material_ids:
            return jsonify({"error": "No material IDs provided"}), 400

        material_ids_list = material_ids.split(",")[:4]

        query = f"""
            SELECT date, material_id, base_unit_of_measure, quantity, price
            FROM material_data 
            WHERE material_id = ANY(%s)
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, (material_ids_list,))
        results = cursor.fetchall()

        if not results:
            return jsonify({"error": "No data found for given material IDs"}), 404

        df = pd.DataFrame(results)
        df["date"] = pd.to_datetime(df["date"])
        df["Year"] = df["date"].dt.year
        df["Month"] = df["date"].dt.strftime("%b")

        all_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        pivot_quantity = df.pivot_table(
            index=["material_id", "Year"],
            columns="Month",
            values="quantity",
            aggfunc="sum",
            fill_value=0
        ).reindex(columns=all_months, fill_value=0).reset_index()

        latest_prices = df.sort_values(by=["Year", "Month"]).groupby("material_id")["price"].last().reset_index()
        unit_df = df.groupby("material_id")["base_unit_of_measure"].first().reset_index()

        final_df = pivot_quantity.merge(latest_prices, on="material_id", how="left")
        final_df = final_df.merge(unit_df, on="material_id", how="left")

        final_df.rename(columns=lambda x: f"quantity_{x}" if x in all_months else x, inplace=True)
        final_df.rename(columns={"price": "last_price"}, inplace=True)

        return jsonify({"data": final_df.to_dict(orient="records")}), 200

    except Exception as e:
        print("Error while fetching data:", e)
        return jsonify({"error": "An error occurred while fetching the data"}), 500
    finally:
        cursor.close()
        conn.close()


# Main function to run the Flask app on Cloud Run
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))  # Cloud Run expects the app to listen on port 8080
    app.run(host="0.0.0.0", port=port)
