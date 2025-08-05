from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.management.queries import QueryIndexManager
import time

# Couchbase connection configuration
COUCHBASE_CONNECTION_STRING = "couchbase://localhost"
COUCHBASE_USERNAME = "CBuser"  # Adjust as needed
COUCHBASE_PASSWORD = "CBpass"       # Adjust as needed
BUCKET_NAME = "test-bucket1"

# Index definitions for optimal query performance
INDEX_DEFINITIONS = [
    {
        "name": "idx_document_type",
        "fields": ["`type`"],
        "description": "Primary index for filtering by document type (patient/test/prescription)",
        "priority": "HIGH"
    },
    {
        "name": "idx_patient_id_composite",
        "fields": ["`type`", "`id`"],
        "description": "Composite index for patient lookups by type and ID",
        "priority": "HIGH"
    },
    {
        "name": "idx_patient_relationships",
        "fields": ["`type`", "`patient_id`"],
        "description": "Find all tests/prescriptions for a specific patient",
        "priority": "HIGH"
    },
    {
        "name": "idx_patient_demographics",
        "fields": ["`type`", "`gender`", "`birth_date_year`"],
        "description": "Patient demographic queries (age groups, gender analysis)",
        "priority": "MEDIUM"
    },
    {
        "name": "idx_patient_name_search",
        "fields": ["`type`", "`name`"],
        "description": "Patient name-based searches",
        "priority": "MEDIUM"
    },
    {
        "name": "idx_test_type_date",
        "fields": ["`type`", "`test_type`", "`result_date`"],
        "description": "Test queries by type and date range",
        "priority": "HIGH"
    },
    {
        "name": "idx_test_patient_date",
        "fields": ["`type`", "`patient_id`", "`result_date`"],
        "description": "Patient's tests ordered by date",
        "priority": "HIGH"
    },
    {
        "name": "idx_prescription_medicine",
        "fields": ["`type`", "`medicine_name`"],
        "description": "Find all prescriptions for specific medications",
        "priority": "MEDIUM"
    },
    {
        "name": "idx_prescription_patient_date",
        "fields": ["`type`", "`patient_id`", "`valid_from`"],
        "description": "Patient's prescriptions ordered by date",
        "priority": "HIGH"
    },
    {
        "name": "idx_prescription_validity",
        "fields": ["`type`", "`valid_from`"],
        "description": "Active prescriptions by validity date",
        "priority": "MEDIUM"
    },
    {
        "name": "idx_largo_code",
        "fields": ["`type`", "`largo_code`"],
        "description": "Prescription lookup by largo code",
        "priority": "LOW"
    }
]

# Sample queries that will benefit from these indexes
SAMPLE_QUERIES = {
    "find_patient_by_id": """
        SELECT * FROM `test-bucket1` 
        WHERE `type` = 'patient' AND `id` = '123456789'
    """,
    
    "find_patient_tests": """
        SELECT * FROM `test-bucket1` 
        WHERE `type` = 'test' AND `patient_id` = '123456789'
        ORDER BY `result_date` DESC
    """,
    
    "find_patient_prescriptions": """
        SELECT * FROM `test-bucket1` 
        WHERE `type` = 'prescription' AND `patient_id` = '123456789'
        ORDER BY `valid_from` DESC
    """,
    
    "find_recent_blood_tests": """
        SELECT * FROM `test-bucket1` 
        WHERE `type` = 'test' 
        AND `test_type` = 'blood_test' 
        AND `result_date` >= '2024-01-01'
        ORDER BY `result_date` DESC
    """,
    
    "find_patients_by_age_gender": """
        SELECT * FROM `test-bucket1` 
        WHERE `type` = 'patient' 
        AND `gender` = 'male' 
        AND `birth_date_year` BETWEEN 1980 AND 1990
    """,
    
    "find_acamol_prescriptions": """
        SELECT p.*, pt.name, pt.birth_date_year
        FROM `test-bucket1` p
        JOIN `test-bucket1` pt ON p.patient_id = pt.id
        WHERE p.`type` = 'prescription' 
        AND pt.`type` = 'patient'
        AND p.medicine_name = 'ACAMOL'
    """,
    
    "patient_complete_profile": """
        SELECT p.*,
               ARRAY_AGG(DISTINCT t) AS tests,
               ARRAY_AGG(DISTINCT pr) AS prescriptions
        FROM `test-bucket1` p
        LEFT JOIN `test-bucket1` t ON t.patient_id = p.id AND t.`type` = 'test'
        LEFT JOIN `test-bucket1` pr ON pr.patient_id = p.id AND pr.`type` = 'prescription'
        WHERE p.`type` = 'patient' AND p.id = '123456789'
        GROUP BY p.*
    """,
    
    "active_prescriptions": """
        SELECT * FROM `test-bucket1` 
        WHERE `type` = 'prescription' 
        AND `valid_from` <= CURRENT_DATE()
        ORDER BY `valid_from` DESC
    """
}

def connect_to_couchbase():
    """Establish connection to Couchbase and return query manager"""
    try:
        # Configure authentication
        auth = PasswordAuthenticator(COUCHBASE_USERNAME, COUCHBASE_PASSWORD)
        
        # Connect to cluster
        cluster = Cluster(COUCHBASE_CONNECTION_STRING, ClusterOptions(auth))
        
        # Get query index manager
        query_manager = cluster.query_indexes()
        
        return cluster, query_manager
    except Exception as e:
        print(f"Error connecting to Couchbase: {e}")
        return None, None

def get_existing_indexes(query_manager):
    """Get list of existing indexes"""
    try:
        indexes = query_manager.get_all_indexes(BUCKET_NAME)
        return [idx.name for idx in indexes]
    except Exception as e:
        print(f"Error getting existing indexes: {e}")
        return []

def create_index(query_manager, index_def):
    """Create a single index"""
    try:
        fields_str = ", ".join(index_def["fields"])
        
        print(f"Creating index: {index_def['name']}")
        print(f"  Fields: {fields_str}")
        print(f"  Description: {index_def['description']}")
        
        query_manager.create_index(
            bucket_name=BUCKET_NAME,
            index_name=index_def["name"],
            fields=index_def["fields"]
        )
        
        return True
    except Exception as e:
        print(f"Error creating index {index_def['name']}: {e}")
        return False

def wait_for_indexes(query_manager, index_names):
    """Wait for indexes to come online"""
    print(f"\nWaiting for {len(index_names)} indexes to build...")
    
    max_wait_time = 300  # 5 minutes
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            query_manager.watch_indexes(
                bucket_name=BUCKET_NAME,
                index_names=index_names,
                timeout=timedelta(seconds=30)
            )
            print("All indexes are online!")
            return True
        except Exception as e:
            print(f"Indexes still building... ({int(time.time() - start_time)}s elapsed)")
            time.sleep(5)
    
    print("Warning: Timeout waiting for indexes to build")
    return False

def create_primary_index(query_manager):
    """Create primary index if it doesn't exist"""
    try:
        print("Creating primary index...")
        query_manager.create_primary_index(BUCKET_NAME)
        return True
    except Exception as e:
        if "already exists" in str(e).lower():
            print("Primary index already exists")
            return True
        else:
            print(f"Error creating primary index: {e}")
            return False

def test_sample_queries(cluster):
    """Test sample queries to verify index performance"""
    print("\n=== Testing Sample Queries ===")
    
    for query_name, query in SAMPLE_QUERIES.items():
        try:
            print(f"\nTesting: {query_name}")
            result = cluster.query(query)
            rows = list(result.rows())
            print(f"  Result: {len(rows)} rows returned")
            
            # Print execution stats if available
            if hasattr(result, 'metadata') and result.metadata():
                metrics = result.metadata().metrics()
                if metrics:
                    print(f"  Execution time: {metrics.execution_time()}")
                    
        except Exception as e:
            print(f"  Error: {e}")

def generate_index_analysis():
    """Generate index usage analysis and recommendations"""
    analysis = """
=== INDEX USAGE ANALYSIS ===

HIGH PRIORITY INDEXES:
1. idx_document_type - Essential for all type-based filtering
2. idx_patient_id_composite - Fast patient lookups by ID
3. idx_patient_relationships - Core relationship queries (tests/prescriptions by patient)
4. idx_test_type_date - Medical analytics and reporting
5. idx_test_patient_date - Patient timeline queries
6. idx_prescription_patient_date - Medication history

MEDIUM PRIORITY INDEXES:
- idx_patient_demographics - Population health analytics
- idx_patient_name_search - User search functionality
- idx_prescription_medicine - Drug utilization studies
- idx_prescription_validity - Active medication tracking

LOW PRIORITY INDEXES:
- idx_largo_code - Administrative lookups

QUERY PATTERNS SUPPORTED:
✓ Find patient by ID
✓ Get all tests for a patient
✓ Get all prescriptions for a patient
✓ Find tests by type and date range
✓ Demographics-based patient searches
✓ Medication-specific queries
✓ Active prescription tracking
✓ Complex joins for complete patient profiles

PERFORMANCE RECOMMENDATIONS:
- Use covering indexes for frequently accessed fields
- Monitor query performance and adjust indexes based on usage patterns
- Consider partitioned indexes for very large datasets
- Use array indexing for test results if complex result queries are needed
"""
    return analysis

def main():
    """Main function to create all indexes"""
    print("=== Couchbase Index Creation Tool ===")
    print(f"Target bucket: {BUCKET_NAME}")
    
    # Connect to Couchbase
    cluster, query_manager = connect_to_couchbase()
    if not cluster or not query_manager:
        print("Failed to connect to Couchbase. Please check your configuration.")
        return
    
    # Create primary index first
    print("\n1. Creating Primary Index...")
    create_primary_index(query_manager)
    
    # Get existing indexes
    print("\n2. Checking Existing Indexes...")
    existing_indexes = get_existing_indexes(query_manager)
    print(f"Found {len(existing_indexes)} existing indexes: {existing_indexes}")
    
    # Create secondary indexes
    print(f"\n3. Creating {len(INDEX_DEFINITIONS)} Secondary Indexes...")
    created_indexes = []
    skipped_indexes = []
    
    for index_def in INDEX_DEFINITIONS:
        if index_def["name"] in existing_indexes:
            print(f"Skipping existing index: {index_def['name']}")
            skipped_indexes.append(index_def["name"])
        else:
            if create_index(query_manager, index_def):
                created_indexes.append(index_def["name"])
    
    # Wait for indexes to build
    if created_indexes:
        print(f"\n4. Waiting for {len(created_indexes)} new indexes to build...")
        wait_for_indexes(query_manager, created_indexes)
    
    # Test sample queries
    print("\n5. Testing Query Performance...")
    test_sample_queries(cluster)
    
    # Print analysis
    print("\n6. Index Analysis and Recommendations...")
    print(generate_index_analysis())
    
    # Summary
    print("\n=== SUMMARY ===")
    print(f"Created indexes: {len(created_indexes)}")
    print(f"Skipped (already exist): {len(skipped_indexes)}")
    print(f"Total indexes: {len(existing_indexes) + len(created_indexes)}")
    
    if created_indexes:
        print(f"\nNew indexes created:")
        for idx in created_indexes:
            print(f"  ✓ {idx}")
    
    print(f"\nYour bucket '{BUCKET_NAME}' is now optimized for medical data queries!")

if __name__ == "__main__":
    main()