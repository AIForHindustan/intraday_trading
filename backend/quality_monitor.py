from datetime import datetime
import time

from singleton_db import DatabaseConnectionManager


class QualityMonitor:
    def __init__(self, db_path: str = "tick_data_production.db") -> None:
        self.db_path = db_path
        self.baseline_quality = {
            "price_quality": 85.0,
            "timestamp_quality": 95.0,
            "depth_quality": 70.0,
        }

    def continuous_monitor(self, check_interval: int = 300) -> None:
        print("🔍 Starting continuous quality monitoring…")
        print(
            f"   Baseline: {self.baseline_quality['price_quality']}% price, "
            f"{self.baseline_quality['timestamp_quality']}% timestamp"
        )
        try:
            while True:
                status = self._check_quality_status()
                self._report_status(status)
                self._alert_on_anomalies(status)
                time.sleep(check_interval)
        except KeyboardInterrupt:
            print("\n🛑 Monitoring stopped")

    def _check_quality_status(self) -> dict:
        with DatabaseConnectionManager.connection_scope(self.db_path) as conn:
            overall = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_records,
                    ROUND(COUNT(CASE WHEN last_price > 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS price_quality,
                    ROUND(COUNT(CASE WHEN exchange_timestamp IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS timestamp_quality,
                    ROUND(COUNT(CASE WHEN bid_1_price IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS depth_quality,
                    COUNT(DISTINCT source_file) AS processed_files,
                    MAX(timestamp) AS latest_data
                FROM tick_data_corrected
                """
            ).fetchone()

            recent = conn.execute(
                """
                SELECT
                    ROUND(COUNT(CASE WHEN last_price > 0 THEN 1 END) * 100.0 / COUNT(*), 2)
                FROM tick_data_corrected
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                """
            ).fetchone()

        return {
            "timestamp": datetime.now(),
            "total_records": overall[0],
            "price_quality": overall[1],
            "timestamp_quality": overall[2],
            "depth_quality": overall[3],
            "processed_files": overall[4],
            "latest_data": overall[5],
            "recent_price_quality": recent[0] if recent and recent[0] is not None else 0.0,
        }

    def _report_status(self, status: dict) -> None:
        print(
            f"\n📊 {status['timestamp'].strftime('%H:%M:%S')} - "
            f"Records: {status['total_records']:,} | Files: {status['processed_files']} | "
            f"Prices: {status['price_quality']}% | Timestamps: {status['timestamp_quality']}% | "
            f"Depth: {status['depth_quality']}%"
        )

    def _alert_on_anomalies(self, status: dict) -> None:
        alerts = []
        if status["price_quality"] < self.baseline_quality["price_quality"]:
            alerts.append(f"❌ Price quality {status['price_quality']}% below baseline")
        if status["timestamp_quality"] < self.baseline_quality["timestamp_quality"]:
            alerts.append(f"❌ Timestamp quality {status['timestamp_quality']}% below baseline")
        if status["recent_price_quality"] < 50:
            alerts.append(
                f"🚨 CRITICAL: Last hour price quality only {status['recent_price_quality']}%"
            )
        if alerts:
            print("🚨 ALERTS:")
            for alert in alerts:
                print(f"   {alert}")


def quick_quality_check() -> bool:
    with DatabaseConnectionManager.connection_scope() as conn:
        quality = conn.execute(
            """
            SELECT
                COUNT(*) AS total_records,
                ROUND(COUNT(CASE WHEN last_price > 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS price_quality,
                ROUND(COUNT(CASE WHEN exchange_timestamp IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS timestamp_quality,
                COUNT(DISTINCT source_file) AS processed_files,
                COUNT(DISTINCT CASE WHEN symbol LIKE 'UNKNOWN_%' THEN instrument_token END) AS unknown_tokens
            FROM tick_data_corrected
            """
        ).fetchone()
    print("📈 QUICK QUALITY CHECK:")
    print(f"   Records: {quality[0]:,}")
    print(f"   Files: {quality[3]}")
    print(f"   Price Quality: {quality[1]}%")
    print(f"   Timestamp Quality: {quality[2]}%")
    print(f"   Unknown Tokens: {quality[4]}")
    return quality[1] >= 85.0


if __name__ == "__main__":
    quick_quality_check()
