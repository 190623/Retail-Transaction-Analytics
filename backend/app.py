import os
import shutil

from flask import Flask, request, jsonify
from flask_cors import CORS

from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import (
    col,
    collect_set,
    desc,
    coalesce,
    expr,
    sum as _sum,
    to_date
)


# --------------------------------------------------
# Spark Configuration
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("RetailAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("Spark Initialized")


# --------------------------------------------------
# Data Helpers
# --------------------------------------------------

def standardize_columns(df):
    original_cols = df.columns
    col_map = {c.strip().lower(): c for c in original_cols}

    if 'price' in col_map and 'unitprice' not in col_map:
        df = df.withColumnRenamed(col_map['price'], 'UnitPrice')

    if 'invoice' in col_map and 'invoiceno' not in col_map:
        df = df.withColumnRenamed(col_map['invoice'], 'InvoiceNo')

    if 'invoicedate' in col_map and col_map['invoicedate'] != 'InvoiceDate':
        df = df.withColumnRenamed(col_map['invoicedate'], 'InvoiceDate')

    return df


def check_columns(df, required):
    missing = [c for c in required if c not in df.columns]

    if missing:
        return (
            f"Missing columns: {', '.join(missing)}. "
            f"Found: {', '.join(df.columns)}"
        )

    return None


def get_common_df(df_path):
    try:
        df = spark.read.csv(
            df_path,
            header=True,
            inferSchema=True
        )

        df = standardize_columns(df)

        return df, None

    except Exception as e:
        return None, str(e)


# --------------------------------------------------
# Analysis Functions
# --------------------------------------------------

def analyze_basket(df_path, min_support=0.01, min_confidence=0.1):

    df, err = get_common_df(df_path)

    if err:
        return None, err

    item_col = 'Description'

    if 'Description' not in df.columns and 'StockCode' in df.columns:
        item_col = 'StockCode'

    err = check_columns(
        df,
        ['InvoiceNo', item_col]
    )

    if err:
        return None, err

    df = df.dropna(
        subset=['InvoiceNo', item_col]
    )

    transactions = df.groupBy("InvoiceNo").agg(
        collect_set(item_col).alias("items")
    )

    fpGrowth = FPGrowth(
        itemsCol="items",
        minSupport=min_support,
        minConfidence=min_confidence
    )

    model = fpGrowth.fit(transactions)

    rules = (
        model.associationRules
        .sort(col("lift").desc())
        .limit(50)
    )

    rules_pdf = rules.toPandas()

    return rules_pdf.to_dict(
        orient='records'
    ), None


def analyze_sales(df_path):

    df, err = get_common_df(df_path)

    if err:
        return None, err

    err = check_columns(
        df,
        ['InvoiceDate', 'Quantity', 'UnitPrice']
    )

    if err:
        return None, err

    df = df.dropna(
        subset=[
            'InvoiceDate',
            'Quantity',
            'UnitPrice'
        ]
    )

    if 'TotalAmount' not in df.columns:
        df = df.withColumn(
            "TotalAmount",
            col("Quantity") * col("UnitPrice")
        )

    df = df.withColumn(
        "ParsedDate",
        coalesce(
            expr(
                "try_to_timestamp("
                "InvoiceDate, "
                "'dd-MM-yyyy HH:mm')"
            ),
            expr(
                "try_to_timestamp("
                "InvoiceDate, "
                "'M/d/yyyy H:m')"
            ),
            expr(
                "try_to_timestamp("
                "InvoiceDate, "
                "'dd/MM/yyyy HH:mm')"
            ),
            expr(
                "try_to_timestamp("
                "InvoiceDate, "
                "'yyyy-MM-dd HH:mm:ss')"
            ),
            expr(
                "try_to_timestamp("
                "InvoiceDate, "
                "'yyyy-MM-dd')"
            ),
            expr(
                "try_to_timestamp("
                "InvoiceDate, "
                "'M/d/yyyy')"
            ),
            expr(
                "try_to_timestamp("
                "InvoiceDate, "
                "'dd/MM/yyyy')"
            )
        )
    )

    df = df.withColumn(
        "Date",
        to_date(col("ParsedDate"))
    )

    df = df.filter(
        col("Date").isNotNull()
    )

    daily_sales = (
        df.groupBy("Date")
        .agg(
            _sum("TotalAmount")
            .alias("DailySales")
        )
        .orderBy("Date")
    )

    pdf = daily_sales.toPandas()

    if pdf.empty:
        return None, "No valid dates found."

    pdf['Date'] = pdf['Date'].astype(str)

    return pdf.to_dict(
        orient='records'
    ), None


def analyze_summary(df_path):

    df, err = get_common_df(df_path)

    if err:
        return None, err

    if 'TotalAmount' not in df.columns:
        df = df.withColumn(
            "TotalAmount",
            col("Quantity") * col("UnitPrice")
        )

    total_revenue = (
        df.agg(_sum("TotalAmount"))
        .collect()[0][0]
    )

    total_transactions = (
        df.select("InvoiceNo")
        .distinct()
        .count()
    )

    total_items = (
        df.agg(_sum("Quantity"))
        .collect()[0][0]
    )

    total_customers = 0

    if "Customer ID" in df.columns:

        total_customers = (
            df.select("Customer ID")
            .distinct()
            .count()
        )

    elif "CustomerID" in df.columns:

        total_customers = (
            df.select("CustomerID")
            .distinct()
            .count()
        )

    return {
        "total_revenue": total_revenue or 0,
        "total_transactions": total_transactions,
        "total_items": total_items or 0,
        "total_customers": total_customers
    }, None


def analyze_top_products(df_path):

    df, err = get_common_df(df_path)

    if err:
        return None, err

    if 'TotalAmount' not in df.columns:
        df = df.withColumn(
            "TotalAmount",
            col("Quantity") * col("UnitPrice")
        )

    item_col = 'Description'

    if 'Description' not in df.columns:
        item_col = 'StockCode'

    top = (
        df.groupBy(item_col)
        .agg(
            _sum("TotalAmount")
            .alias("Revenue")
        )
        .orderBy(desc("Revenue"))
        .limit(5)
    )

    return top.toPandas().to_dict(
        orient='records'
    ), None


def analyze_by_country(df_path):

    df, err = get_common_df(df_path)

    if err:
        return None, err

    if 'Country' not in df.columns:
        return [], "'Country' column missing"

    if 'TotalAmount' not in df.columns:
        df = df.withColumn(
            "TotalAmount",
            col("Quantity") * col("UnitPrice")
        )

    countries = (
        df.groupBy("Country")
        .agg(
            _sum("TotalAmount")
            .alias("Revenue")
        )
        .orderBy(desc("Revenue"))
        .limit(10)
    )

    return countries.toPandas().to_dict(
        orient='records'
    ), None


# --------------------------------------------------
# Flask Application
# --------------------------------------------------

app = Flask(__name__)

CORS(
    app,
    resources={
        r"/*": {
            "origins": "*"
        }
    }
)


# Docker-friendly upload directory
UPLOAD_FOLDER = "/app/uploads"

os.makedirs(
    UPLOAD_FOLDER,
    exist_ok=True
)

CURRENT_FILE = None


# --------------------------------------------------
# API Routes
# --------------------------------------------------

@app.route('/')
def home():

    return jsonify({
        "status": "running"
    })


@app.route(
    '/upload',
    methods=['POST']
)
def upload_file():

    global CURRENT_FILE

    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

    for filename in os.listdir(UPLOAD_FOLDER):
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)

    if 'file' not in request.files:

        return jsonify({
            "error": "No file part"
        }), 400

    file = request.files['file']

    if file.filename == '':

        return jsonify({
            "error": "No selected file"
        }), 400

    filepath = os.path.join(
        UPLOAD_FOLDER,
        "transaction_data.csv"
    )

    file.save(filepath)

    CURRENT_FILE = filepath

    return jsonify({
        "message": "File uploaded successfully",
        "path": filepath
    })


@app.route(
    '/analyze/basket',
    methods=['GET']
)
def get_basket_analysis():

    global CURRENT_FILE

    if not CURRENT_FILE:

        return jsonify({
            "error": "No file uploaded"
        }), 400

    min_sup = float(
        request.args.get(
            'min_support',
            0.01
        )
    )

    min_conf = float(
        request.args.get(
            'min_confidence',
            0.1
        )
    )

    results, error = analyze_basket(
        CURRENT_FILE,
        min_sup,
        min_conf
    )

    if error:

        return jsonify({
            "error": error
        }), 500

    return jsonify(results)


@app.route(
    '/analyze/sales',
    methods=['GET']
)
def get_sales_analysis():

    global CURRENT_FILE

    if not CURRENT_FILE:

        return jsonify({
            "error": "No file uploaded"
        }), 400

    results, error = analyze_sales(
        CURRENT_FILE
    )

    if error:

        return jsonify({
            "error": error
        }), 500

    return jsonify(results)


@app.route(
    '/analyze/summary',
    methods=['GET']
)
def get_summary():

    global CURRENT_FILE

    if not CURRENT_FILE:

        return jsonify({
            "error": "No file uploaded"
        }), 400

    results, error = analyze_summary(
        CURRENT_FILE
    )

    if error:

        return jsonify({
            "error": error
        }), 500

    return jsonify(results)


@app.route(
    '/analyze/top_products',
    methods=['GET']
)
def get_top_products():

    global CURRENT_FILE

    if not CURRENT_FILE:

        return jsonify({
            "error": "No file uploaded"
        }), 400

    results, error = analyze_top_products(
        CURRENT_FILE
    )

    if error:

        return jsonify({
            "error": error
        }), 500

    return jsonify(results)


@app.route(
    '/analyze/by_country',
    methods=['GET']
)
def get_by_country():

    global CURRENT_FILE

    if not CURRENT_FILE:

        return jsonify({
            "error": "No file uploaded"
        }), 400

    results, error = analyze_by_country(
        CURRENT_FILE
    )

    if error:

        return jsonify({
            "error": error
        }), 500

    return jsonify(results)


# --------------------------------------------------
# Start Application
# --------------------------------------------------

if __name__ == '__main__':

    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False
    )