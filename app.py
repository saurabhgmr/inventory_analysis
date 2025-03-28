from flask import Flask, request, jsonify
import os
import psycopg2
import json
import calendar

# Environment variables for database credentials
DB_NAME = os.getenv("DB_NAME", "material_db")
DB_USER = os.getenv("DB_USER", "replicator")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Ant-admin@123")
DB_HOST = os.getenv("DB_HOST", "34.100.200.180")
DB_PORT = int(os.getenv("DB_PORT", 5432))

# Define Flask App
app = Flask(__name__)

# Establish Database Connection
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Inventory Analysis API
@app.route("/inventory-analysis", methods=["GET"])
def inventory_analysis():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT material_id, AVG(quantity) AS avg_monthly_turnover
            FROM material_data
            GROUP BY material_id
        """)
        consumption_data = cursor.fetchall()

        cursor.execute("""
            SELECT material AS material_id, MAX(unrestricted) AS current_stock
            FROM material_stock
            GROUP BY material
        """)
        stock_data = cursor.fetchall()

        cursor.close()
        conn.close()

        turnover_rates = {row[0]: row[1] for row in consumption_data}
        current_stock_levels = {row[0]: row[1] for row in stock_data}

        over_stocked = []
        under_stocked = []
        fast_moving = []
        slow_moving = []

        STOCK_THRESHOLD_MONTHS = 2
        FAST_MOVING_THRESHOLD = 2

        for material_id, stock in current_stock_levels.items():
            turnover_rate = turnover_rates.get(material_id, 0)

            if stock > turnover_rate * STOCK_THRESHOLD_MONTHS:
                over_stocked.append({"material_id": material_id, "current_stock": stock, "turnover_rate": turnover_rate})
            elif stock < turnover_rate * STOCK_THRESHOLD_MONTHS:
                under_stocked.append({"material_id": material_id, "current_stock": stock, "turnover_rate": turnover_rate})

            if turnover_rate >= FAST_MOVING_THRESHOLD:
                fast_moving.append({"material_id": material_id, "turnover_rate": turnover_rate})
            elif turnover_rate < 2:
                slow_moving.append({"material_id": material_id, "turnover_rate": turnover_rate})

        response = {
            "over_stocked": over_stocked,
            "under_stocked": under_stocked,
            "fast_moving": fast_moving,
            "slow_moving": slow_moving
        }
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/average-consumption", methods=["GET"])
def average_consumption():
    material_id = request.args.get("material_id")
    
    if not material_id:
        return jsonify({"error": "Material ID is required"}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT EXTRACT(MONTH FROM DATE) AS month, 
                   COALESCE(AVG(quantity), 0) AS avg_monthly_consumption
            FROM material_data
            WHERE material_id = %s
            GROUP BY month
            ORDER BY month
        """, (material_id,))
        
        db_result = cursor.fetchall()
        cursor.close()
        conn.close()

        avg_consumption = {int(row[0]): int(round(row[1])) for row in db_result}

        result = []
        for month_num in range(1, 13):
            result.append({
                "month": calendar.month_name[month_num],
                "avg_consumption": avg_consumption.get(month_num, 0)
            })

        return jsonify({"material_id": material_id, "data": result}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/average-breakdowns", methods=["GET"])
def average_breakdowns():
    equipment = request.args.get("equipment")
    
    if not equipment:
        return jsonify({"error": "Equipment Number is required"}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT EXTRACT(MONTH FROM malf_start) AS month, 
                   COUNT(*) AS breakdown_count
            FROM breakdown_data
            WHERE equipment = %s
            GROUP BY month
            ORDER BY month
        """, (equipment,))
        
        db_result = cursor.fetchall()
        cursor.close()
        conn.close()

        breakdown_counts = {int(row[0]): int(row[1]) for row in db_result}

        result = []
        for month_num in range(1, 13):
            result.append({
                "month": calendar.month_abbr[month_num],
                "avg_breakdowns": breakdown_counts.get(month_num, 0)
            })

        return jsonify({"equipment": equipment, "data": result}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run Flask App
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
