# 📘 Design Documentation: CDI Bonus Allocation System

## 1. Overview

This document describes the **design and implementation** of the **CDI Bonus Allocation System**, a data product responsible for calculating and distributing **daily interest (CDI bonus)** to user wallets. The system processes **raw Change Data Capture (CDC) files** to track wallet balances over time and applies business rules to determine interest eligibility.

The solution was developed using **Apache Spark** for distributed data processing, with a focus on **reliability, accuracy, and maintainability**. It was designed to operate in a **production-level environment**, ensuring transparency and traceability of all operations.

---

## 2. Functional Requirements and Implementation

The following table outlines how each functional requirement was implemented and the reasoning behind the design choices:

| **Requirement** | **Implementation** | **Justification** |
|-----------------|--------------------|-------------------|
| Track wallet balance history | Spark is used to process daily raw CDC files containing each user's wallet balance. | Enables precise interest calculations and supports auditing. |
| Calculate interest only on balances above \$100 | Balances below \$100 are identified before applying interest logic. | Ensures compliance with business rules and avoids unnecessary calculations. |
| Apply interest only to idle balances for at least 24 hours | The system compares timestamp records of balance changes to detect inactivity. | Prevents interest from being applied to frequently moved funds. |
| Support daily changes in interest rates | A dynamic table of interest rates is joined with balance data by date. | Allows flexibility in adjusting rates over time. |
| Pay interest daily to eligible users | Spark generates one transaction record per user per day, which is then written to the output layer. | Ensures consistent and traceable interest payments. |

---

## 3. Non-Functional Requirements and Design Choices

This section describes how the system addresses non-functional requirements and the reasoning behind key architectural and technical decisions.

| **Requirement** | **Implementation** | **Justification** |
|------------------|---------------------|--------------------|
| **Accuracy in financial calculations** | Uses Spark’s `DecimalType` and banking-standard rounding logic. | Prevents rounding errors and ensures financial correctness. |
| **Scalability** | Apache Spark processes large volumes of wallet data in parallel. | Supports future growth of user base and data volume. |
| **Maintainability** | Modular codebase with clear separation of ETL stages and reusable components. | Facilitates debugging, testing, and future improvements. |

---

## 4. Architecture Overview

The **CDI Bonus Allocation System** was designed using a modular, scalable, and fault-tolerant architecture. Below is an overview of the **main components** and their functions:

### 🧱 Architecture Components

#### **Tools Used**
- **Docker**: Development environment setup
- **Apache Spark**: Reads and analyzes files into structured DataFrames.
- **SQLite**: Lightweight and efficient database.

#### **Bronze Layer (Ingestion Layer)**
- Ingests raw CDC (Change Data Capture) files containing wallet updates.

#### **Silver Layer (Processing Layer)**
- Aggregates daily CDC events of wallet balances, providing historical balance visibility per day.
- Applies business rules to determine eligible balances.
- Calculates interest based on the daily rate.

#### **Storage**
- **Intermediate Outputs**: Stored in Parquet format for auditing and reprocessing.
- **Final Output**: Interest transactions are recorded in an SQLite database used as the transactional base.

---

## 🗺️ Data Flow Summary

1. **CDC Files → Spark Ingestion**
2. **Wallet History Table → Interest Calculation**
3. **Interest Records → Output Table**
4. **Output Table → Production Database**

## 🚀 Improvement Points

1 - I could have used Airflow to orchestrate the data. However, I opted for a sequence of calls to Python scripts instead.

2 - I could have used a more robust database like PostgreSQL, but SQLite is lightweight and works well for demonstrating the solution.

3 - I could have decoupled the code further by using inheritance and polymorphism, but time was short.
