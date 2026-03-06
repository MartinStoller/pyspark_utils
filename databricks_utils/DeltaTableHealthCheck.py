import time
from datetime import datetime

from delta import DeltaTable
from pyspark.sql.functions import col

from databricks_utils.spark_session_and_environment import is_databricks


class DeltaTableHealthCheck:
    """Utility class to analyze Delta table health and recommend maintenance."""

    def __init__(self, spark, table_path):
        self.spark = spark
        self.table_path = table_path
        self.delta_table = DeltaTable.forPath(spark, table_path)

    def get_file_statistics(self):
        """Get statistics about data files."""
        if is_databricks():
            detail = self.spark.sql(f"DESCRIBE DETAIL delta.`{self.table_path}`").collect()[0]
            num_files = detail["numFiles"]
            total_size = detail["sizeInBytes"]
            if num_files == 0:
                return {"num_files": 0, "total_size_mb": 0, "avg_size_mb": 0, "min_size_mb": 0, "max_size_mb": 0}
            return {
                "num_files": num_files,
                "total_size_mb": total_size / (1024 * 1024),
                "avg_size_mb": (total_size / num_files) / (1024 * 1024),
                "min_size_mb": 0,  # Not available via DESCRIBE DETAIL
                "max_size_mb": 0,
            }
        else:
            import os

            files = []
            for root, dirs, filenames in os.walk(self.table_path):
                if "_delta_log" in root:
                    continue
                for f in filenames:
                    if f.endswith(".parquet"):
                        path = os.path.join(root, f)
                        files.append({"name": f, "size_bytes": os.path.getsize(path)})

            if not files:
                return {"num_files": 0, "total_size_mb": 0, "avg_size_mb": 0, "min_size_mb": 0, "max_size_mb": 0}

            sizes = [f["size_bytes"] for f in files]
            return {
                "num_files": len(files),
                "total_size_mb": sum(sizes) / (1024 * 1024),
                "avg_size_mb": (sum(sizes) / len(sizes)) / (1024 * 1024),
                "min_size_mb": min(sizes) / (1024 * 1024),
                "max_size_mb": max(sizes) / (1024 * 1024),
            }

    def get_version_info(self):
        """Get version and history information."""
        history = self.delta_table.history()
        latest = history.orderBy(col("version").desc()).first()
        oldest = history.orderBy(col("version").asc()).first()

        return {
            "current_version": latest["version"],
            "oldest_version": oldest["version"],
            "total_versions": history.count(),
            "latest_operation": latest["operation"],
            "latest_timestamp": latest["timestamp"],
        }

    def analyze_and_recommend(self):
        """Analyze table and provide maintenance recommendations."""
        file_stats = self.get_file_statistics()
        version_info = self.get_version_info()

        recommendations = []

        if file_stats["num_files"] > 10 and file_stats["avg_size_mb"] < 64:
            recommendations.append(
                {
                    "issue": "Small File Problem",
                    "severity": "HIGH",
                    "action": "Run OPTIMIZE to compact small files",
                    "details": f"{file_stats['num_files']} files with avg size {file_stats['avg_size_mb']:.2f} MB",
                }
            )

        if file_stats["max_size_mb"] > 0 and file_stats["min_size_mb"] / file_stats["max_size_mb"] < 0.1:
            recommendations.append(
                {
                    "issue": "Uneven File Sizes",
                    "severity": "MEDIUM",
                    "action": "Run OPTIMIZE to balance file sizes",
                    "details": f"Min: {file_stats['min_size_mb']:.2f} MB, Max: {file_stats['max_size_mb']:.2f} MB",
                }
            )

        if version_info["total_versions"] > 100:
            recommendations.append(
                {
                    "issue": "High Version Count",
                    "severity": "LOW",
                    "action": "Consider running VACUUM to clean up old versions",
                    "details": f"{version_info['total_versions']} versions in history",
                }
            )

        return {
            "file_statistics": file_stats,
            "version_info": version_info,
            "recommendations": recommendations,
            "health_score": self._calculate_health_score(file_stats, version_info, recommendations),
        }

    def _calculate_health_score(self, file_stats, version_info, recommendations):
        """Calculate a health score from 0-100."""
        score = 100

        for rec in recommendations:
            if rec["severity"] == "HIGH":
                score -= 30
            elif rec["severity"] == "MEDIUM":
                score -= 15
            elif rec["severity"] == "LOW":
                score -= 5

        return max(0, score)


def run_maintenance_workflow(spark, table_path, options=None):
    """
    Run a complete maintenance workflow on a Delta table.

    Args:
        spark: SparkSession
        table_path: Path to Delta table
        options: Dict with optional settings:
            - run_optimize: bool (default: True)
            - run_vacuum: bool (default: True)
            - vacuum_retain_hours: int (default: 168)
            - zorder_columns: list (default: None)
            - analyze_stats: bool (default: True)
    """
    options = options or {}
    run_optimize = options.get("run_optimize", True)
    run_vacuum = options.get("run_vacuum", True)
    vacuum_retain_hours = options.get("vacuum_retain_hours", 168)
    zorder_columns = options.get("zorder_columns", None)
    analyze_stats = options.get("analyze_stats", True)

    results = {"start_time": datetime.now(), "steps": []}

    delta_table = DeltaTable.forPath(spark, table_path)

    print("\n📊 Step 1: Running health check...")
    health_checker = DeltaTableHealthCheck(spark, table_path)
    health_report = health_checker.analyze_and_recommend()
    results["initial_health"] = health_report
    print(f"   Initial health score: {health_report['health_score']}/100")

    if run_optimize:
        print("\n🔧 Step 2: Running OPTIMIZE...")
        start = time.time()

        if zorder_columns:
            print(f"   Z-ORDER columns: {zorder_columns}")
            _ = delta_table.optimize().executeZOrderBy(*zorder_columns)
        else:
            _ = delta_table.optimize().executeCompaction()

        elapsed = time.time() - start
        results["steps"].append({"name": "OPTIMIZE", "duration_seconds": elapsed, "success": True})
        print(f"   ✓ Completed in {elapsed:.2f} seconds")

    if run_vacuum:
        print(f"\n🧹 Step 3: Running VACUUM (retain {vacuum_retain_hours} hours)...")
        start = time.time()

        spark.sql(f"VACUUM delta.`{table_path}` RETAIN {vacuum_retain_hours} HOURS")

        elapsed = time.time() - start
        results["steps"].append({"name": "VACUUM", "duration_seconds": elapsed, "success": True})
        print(f"   ✓ Completed in {elapsed:.2f} seconds")

    if analyze_stats:
        print("\n📈 Step 4: Computing statistics...")
        start = time.time()

        spark.sql(f"ANALYZE TABLE delta.`{table_path}` COMPUTE STATISTICS")

        elapsed = time.time() - start
        results["steps"].append({"name": "ANALYZE", "duration_seconds": elapsed, "success": True})
        print(f"   ✓ Completed in {elapsed:.2f} seconds")

    print("\n📊 Step 5: Running final health check...")
    health_report_final = health_checker.analyze_and_recommend()
    results["final_health"] = health_report_final
    print(f"   Final health score: {health_report_final['health_score']}/100")

    results["end_time"] = datetime.now()
    results["total_duration"] = (results["end_time"] - results["start_time"]).total_seconds()

    return results
