# Đề xuất Dự án: E-Commerce Real-Time & Batch Data Lakehouse Platform

Tài liệu này trình bày ý tưởng, kiến trúc chi tiết và các thành phần công nghệ cho một dự án Data Engineering hoàn chỉnh, có quy mô lớn, tích hợp đầy đủ các thành phần từ Ingestion, Processing, Storage, Orchestration đến Data Quality, Governance và Monitoring. Dự án này được thiết kế để sẵn sàng đưa vào môi trường Production (Production-Ready).

---

## 1. Tổng quan Dự án (Project Overview)

Mục tiêu là xây dựng một nền tảng phân tích dữ liệu tích hợp cho một ứng dụng Thương mại điện tử (E-Commerce) quy mô lớn. Hệ thống cần xử lý song song hai luồng dữ liệu:

1. **Luồng Real-time (Stream)**: Theo dõi hành vi clickstream của người dùng trên web/app (lượt xem sản phẩm, thêm vào giỏ hàng, tìm kiếm) để phục vụ phát hiện hành vi gian lận (Fraud Detection) và gợi ý sản phẩm ngay lập tức (Real-time Recommendation).
2. **Luồng Batch (Batch & Change Data Capture - CDC)**: Thu thập dữ liệu giao dịch, thông tin khách hàng, trạng thái kho hàng từ các cơ sở dữ liệu vận hành (OLTP) để phục vụ báo cáo phân tích kinh doanh (BI) định kỳ, tính toán vòng đời khách hàng (LTV) và phân tích phễu chuyển đổi (Conversion Funnel).

---

## 2. Kiến trúc Hệ thống (Architecture Diagram)

```mermaid
graph TD
    %% Sources
    subgraph Data Sources
        C[Web/App Clickstream] -->|JSON Events| K[Apache Kafka / Redpanda]
        DB[(OLTP Postgres)] -->|WAL Logs| DBZ[Debezium CDC]
        DBZ -->|Avro Events| K
    end

    %% Ingestion & Stream Processing
    subgraph Real-Time Processing
        K -->|Stream| Flink[Apache Flink / Spark Streaming]
        Flink -->|Alerts/Fraud| Redis[(Redis - Cache/Real-time DB)]
        Flink -->|Transformed Events| Lake[Apache Iceberg / MinIO]
    end

    %% Storage & Lakehouse
    subgraph Data Lakehouse (S3/GCS/MinIO)
        Lake --> Raw[Raw Zone - Parquet]
        Raw --> Silver[Silver Zone - Enriched/Cleaned]
        Silver --> Gold[Gold Zone - Aggregated/Models]
    end

    %% Batch Processing & Query
    subgraph Processing & Query Engine
        Gold --> Trino[Trino / Presto]
        dbt[dbt - Data Build Tool] -.->|Run Queries & Build Gold| Trino
        Trino --> Superset[Apache Superset / Metabase]
    end

    %% Orchestration & Governance
    subgraph Orchestration & Quality
        Dagster[Dagster / Apache Airflow] -->|Orchestrate| DBZ
        Dagster -->|Trigger Batch Job| Spark[Apache Spark]
        Dagster -->|Run Tests| GE[Great Expectations / Soda]
        Dagster -->|Trigger Models| dbt
        OpenLineage[OpenLineage] -.->|Metadata & Lineage| Dagster
    end

    %% Infrastructure & DevOps
    subgraph Platform & Monitoring
        Terraform[Terraform] -.->|Deploy Infra| K8s[Kubernetes GKE/EKS]
        Prometheus[Prometheus & Grafana] -.->|Metrics & Alerts| K8s
    end
```

---

## 3. Chi tiết các thành phần công nghệ (Tech Stack)

### 3.1. Ingestion Layer (Thu thập dữ liệu)

- **Apache Kafka / Redpanda**: Broker xử lý thông điệp chịu lỗi cao, throughput lớn. Đóng vai trò là xương sống (backbone) trung chuyển dữ liệu thời gian thực.
- **Debezium (CDC)**: Kết nối với cơ sở dữ liệu PostgreSQL chính (OLTP) để capture mọi thay đổi (Insert, Update, Delete) dưới dạng event streaming mà không làm ảnh hưởng đến hiệu năng của DB nguồn.

### 3.2. Processing Layer (Xử lý dữ liệu)

- **Real-time (Stream Processing)**: **Apache Flink** hoặc **Spark Structured Streaming** để xử lý các phép toán trên Window (Session Window, Sliding Window) như đếm số click trong 5 phút gần nhất, phát hiện click tặc (fraud).
- **Batch Processing**: **Apache Spark (PySpark)** chạy trên Kubernetes để xử lý lượng lớn dữ liệu lịch sử định kỳ, thực hiện các tác vụ nặng như khử trùng lặp (de-duplication), liên kết bảng (large-scale joins).

### 3.3. Storage Layer (Lakehouse Architecture)

- **Format: Apache Iceberg (hoặc Delta Lake)**: Cung cấp tính năng ACID Transactions trên Object Storage, cho phép Time Travel (truy vấn dữ liệu tại một thời điểm trong quá khứ), Schema Evolution (tiến hóa schema mà không cần rewrite lại bảng) và tối ưu hóa việc phân vùng dữ liệu.
- **Object Storage**: AWS S3, Google Cloud Storage (GCS) hoặc MinIO (Self-hosted trên Kubernetes).
- **Phân chia Zone**:
  - **Raw (Bronze)**: Lưu giữ nguyên bản dữ liệu thô nhận từ Kafka và CDC.
  - **Silver**: Dữ liệu đã được chuẩn hóa schema, làm sạch, khử trùng lặp và enrich thông tin (ví dụ: chuyển đổi timezone, phân loại thiết bị từ User-Agent).
  - **Gold**: Dữ liệu đã được tổng hợp (Aggregated data models), sẵn sàng cho BI dashboard hoặc làm Features cho ML.

### 3.4. Transformation & Serving Layer (Biến đổi & Truy vấn)

- **dbt (Data Build Tool)**: Quản lý các câu lệnh SQL biến đổi dữ liệu từ Silver sang Gold. Tích hợp sẵn tính năng lập tài liệu (Documentation) và kiểm thử dữ liệu (Data Testing).
- **Trino (Presto)**: Công cụ phân tích truy vấn phân tán (Distributed SQL Query Engine), cho phép truy vấn trực tiếp trên Apache Iceberg với tốc độ cực nhanh mà không cần load dữ liệu vào một Data Warehouse truyền thống.
- **Apache Superset / Metabase**: Công cụ trực quan hóa dữ liệu (BI Dashboard) mã nguồn mở giúp theo dõi các chỉ số kinh doanh như GMV, DAU/MAU, Conversion Rate theo thời gian thực và theo ngày.

### 3.5. Orchestration & Metadata (Điều phối & Quản trị)

- **Dagster (khuyên dùng cho dự án mới) hoặc Apache Airflow**: Điều phối và lên lịch toàn bộ pipeline (ví dụ: kích hoạt job Spark sau khi data ingestion hoàn tất, sau đó chạy dbt, rồi chạy test chất lượng dữ liệu).
- **Great Expectations / Soda**: Tự động hóa việc kiểm tra chất lượng dữ liệu (Data Quality) ở mỗi bước: đảm bảo trường `user_id` không bị Null, `order_amount` phải dương, tỷ lệ dữ liệu lỗi không vượt quá 1%.
- **OpenLineage + Marquez**: Theo dõi vết dữ liệu (Data Lineage) từ bảng nguồn đến bảng đích để dễ dàng debug khi báo cáo BI bị sai lệch số liệu.

### 3.6. Infrastructure & Operations (Hạ tầng & Vận hành)

- **Terraform**: Định nghĩa toàn bộ tài nguyên cloud (VPC, IAM, Kubernetes Cluster, GCS Buckets, RDS) dưới dạng mã nguồn (Infrastructure as Code - IaC).
- **Kubernetes (K8s)**: Triển khai các thành phần như Spark Jobs, Trino Workers, Flink TaskManagers, Airflow/Dagster bằng Helm Charts nhằm tăng tính co giãn (Scalability) và tối ưu hóa chi phí phần cứng.
- **Prometheus & Grafana**: Giám sát hiệu năng hệ thống (CPU, Memory, Kafka Consumer Lag, Spark Job duration).

---

## 4. Các bài toán nghiệp vụ cụ thể (Business Use Cases)

1. **Real-Time Cart Abandonment Detection**: Nhận biết người dùng đã thêm sản phẩm vào giỏ hàng nhưng không thanh toán sau 15 phút. Hệ thống Flink sẽ gửi một sự kiện sang hệ thống CRM để gửi Notification/Email khuyến mãi kích cầu.
2. **User Session Reconstruction**: Gom nhóm các sự kiện clickstream riêng lẻ của người dùng thành các Session hoàn chỉnh để phân tích đường đi của khách hàng trước khi họ mua hàng (User Journey).
3. **Slowly Changing Dimension (SCD Type 2)**: Sử dụng CDC kết hợp dbt để theo dõi lịch sử thay đổi thông tin của sản phẩm (ví dụ: giá sản phẩm thay đổi theo thời gian) phục vụ báo cáo doanh thu chính xác tại thời điểm bán.

---

## 5. Kế hoạch Triển khai từng bước (Phase-by-Phase Execution Plan)

- **Phase 1: Local Development & Sandbox (Docker Compose)**: Dựng mockup Kafka, Postgres, MinIO, Trino và dbt trên Local để kiểm thử các logic cơ bản.
- **Phase 2: Infrastructure Setup (Terraform & K8s)**: Khởi tạo cluster trên Cloud (AWS/GCP), cấu hình mạng, bảo mật và deploy Kubernetes Helm Charts cho Kafka, Spark.
- **Phase 3: Real-Time & CDC Ingestion**: Viết pipeline CDC từ Postgres sang Kafka và Flink Streaming pipeline ghi dữ liệu vào Raw Iceberg Tables.
- **Phase 4: Batch Transformation & Quality**: Viết các mô hình dbt trên Trino để xử lý từ Bronze -> Silver -> Gold. Cấu hình kiểm thử với Great Expectations.
- **Phase 5: Orchestration & BI**: Thiết lập Dagster/Airflow để tự động hóa lịch trình chạy hàng giờ/ngày, tích hợp Superset để vẽ dashboard.
- **Phase 6: Monitoring, CI/CD & Security**: Thiết lập GitHub Actions tự động CI/CD cho dbt models và Terraform scripts. Cấu hình Prometheus alert nếu Kafka Consumer Lag quá cao.

---

## 6. Các điểm nhấn giúp dự án ghi điểm (Wow Factors / Production-Ready Checklist)

- [ ] **Zero-Data-Loss**: Cấu hình Kafka `acks=all`, Flink Checkpointing và transactional writes để đảm bảo không mất mát dữ liệu khi hệ thống gặp sự cố.
- [ ] **Schema Registry Integration**: Sử dụng Avro Schema Registry để kiểm soát tính tương thích của Schema giữa Producer và Consumer, tránh lỗi hệ thống khi ứng dụng OLTP thay đổi cấu trúc bảng.
- [ ] **Idempotent Ingestion**: Thiết kế pipeline để khi một job Spark/Flink bị crash và restart lại thì dữ liệu ghi vào Iceberg không bị trùng lặp.
- [ ] **Cost Optimization**: Cấu hình chính sách lưu trữ (Lifecycle Policy) trên S3/GCS để chuyển các phân vùng Iceberg cũ (> 1 năm) sang tầng lưu trữ giá rẻ (Glacier/Coldline).
- [ ] **Data Security / Masking**: Sử dụng Trino/dbt để ẩn (masking) các trường dữ liệu nhạy cảm (PII) như số điện thoại, email của khách hàng trước khi chia sẻ cho phân tích viên.
