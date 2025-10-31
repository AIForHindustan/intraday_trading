from singleton_db import DatabaseConnectionManager

def generate_quality_dashboard():
    with DatabaseConnectionManager.connection_scope() as conn:
        print("📊 DATA QUALITY DASHBOARD")
        print("=" * 80)

        overall = conn.execute(
            """
            SELECT
                COUNT(*) AS total_records,
                COUNT(DISTINCT instrument_token) AS unique_instruments,
                COUNT(DISTINCT source_file) AS processed_files,
                MIN(exchange_timestamp) AS data_start,
                MAX(exchange_timestamp) AS data_end
            FROM tick_data_corrected
            """
        ).fetchone()

        print("📈 Overall Statistics:")
        print(f"   Total Records: {overall[0]:,}")
        print(f"   Unique Instruments: {overall[1]:,}")
        print(f"   Processed Files: {overall[2]}")
        print(f"   Data Range: {overall[3]} to {overall[4]}")

        quality = conn.execute(
            """
            SELECT
                ROUND(COUNT(CASE WHEN last_price > 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS price_quality_pct,
                ROUND(COUNT(CASE WHEN exchange_timestamp IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS timestamp_quality_pct,
                ROUND(COUNT(CASE WHEN bid_1_price IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS depth_quality_pct,
                COUNT(CASE WHEN last_price <= 0 THEN 1 END) AS invalid_prices,
                COUNT(CASE WHEN exchange_timestamp IS NULL THEN 1 END) AS missing_timestamps
            FROM tick_data_corrected
            """
        ).fetchone()

        print("\n🎯 Data Quality Metrics:")
        print(f"   Price Quality: {quality[0]}% valid")
        print(f"   Timestamp Quality: {quality[1]}% valid")
        print(f"   Depth Quality: {quality[2]}% valid")
        print(f"   Invalid Prices: {quality[3]:,}")
        print(f"   Missing Timestamps: {quality[4]:,}")

        packet_df = conn.execute(
            """
            SELECT
                packet_type,
                COUNT(*) AS records,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tick_data_corrected), 2) AS pct,
                ROUND(AVG(CASE WHEN last_price > 0 THEN 1 ELSE 0 END) * 100, 2) AS price_quality
            FROM tick_data_corrected
            GROUP BY packet_type
            ORDER BY records DESC
            """
        ).fetchdf()

        print("\n📦 Packet Type Analysis:")
        for _, row in packet_df.iterrows():
            print(
                f"   {row['packet_type']}: {row['records']:,} ({row['pct']}%) - "
                f"{row['price_quality']}% valid prices"
            )

        top_instruments = conn.execute(
            """
            SELECT
                symbol,
                COUNT(*) AS tick_count,
                ROUND(COUNT(CASE WHEN last_price > 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS quality_score,
                COUNT(CASE WHEN bid_1_price IS NOT NULL THEN 1 END) AS depth_records
            FROM tick_data_corrected
            GROUP BY symbol
            HAVING tick_count > 1000
            ORDER BY quality_score DESC
            LIMIT 10
            """
        ).fetchdf()

        print("\n🏆 Top Instruments by Quality:")
        for _, row in top_instruments.iterrows():
            print(
                f"   {row['symbol']}: {row['tick_count']:,} ticks, "
                f"{row['quality_score']}% quality, {row['depth_records']} depth records"
            )


if __name__ == "__main__":
    generate_quality_dashboard()
