Home assignment – Senior Data engineer 

**Home Assignment – Senior Data Engineer**

**Context**

You are asked to design a small internal Python package that manages database access for a team of engineers.

The system:

* Will be used by multiple engineers and automated jobs.  
* May run multiple instances in parallel.  
* Will run on cloud spot instances and may be terminated at any time.  
* Must remain safe, consistent, and stateless.

Your solution should emphasize:

* Clean architectural boundaries  
* Abstraction and extensibility  
* Correctness under failure  
* Concurrency safety  
* Operational maturity

Keep the implementation concise. Focus on design quality.

---

**Task 1 – Database Service Design**

Design and implement a Python package that provides database access through a well-structured interface, choose any database as choise for the excrise. 

**Requirements**

1. The package must:  
   * Manage connections  
   * Support transactions  
   * Support batch inserts  
   * Support upserts  
   * Be safe for concurrent usage

2. The design must:  
   * Allow supporting multiple database types in the future  
   * Prevent business logic from depending on a specific DB engine  
3. Since we want to be cost effiecent we want to use spot instances for compute  
4. Calling code must not be aware of the concrete DB implementation.

**Deliverables**

* Code implementation  
* Short explanation (1–2 paragraphs):  
  * How the design supports extension to additional databases  
  * Why the design is stateless  
  * How transaction boundaries are handled

---

**Task 2 – CSV Ingestion Under Reliability Constraints**

You are given a CSV file containing usage data.

**Requirements**

1. Design an appropriate database schema.  
   * Include primary keys, constraints, and indexes.  
   * Briefly justify your design choices.  
2. Implement a Python ingestion script that:  
   * Reads the CSV file  
   * Performs necessary transformations  
   * Inserts the data using your database service  
3. The CSV may contain up to 20M rows.  
   * The ingestion must not load the entire file into memory.  
   * Explain how your approach scales.

---

**Reliability Constraints**

The ingestion service:

* May be terminated at any time.  
* May be restarted automatically.  
* May run multiple instances in parallel.  
* Must remain stateless.

The system must:

* Be safe to retry.  
* Not produce duplicate records.  
* Not leave partially written logical entities.  
* Preserve clean architectural boundaries.

**In your explanation, describe:**

1. In which realistic scenarios duplication may occur.  
2. How your design prevents duplication.  
3. What database-level guarantees you enforce.  
4. How transaction boundaries are defined.  
5. What happens if the process is terminated mid-batch.

---

**Task 3 – External API Integration**

We charge customers in USD, while usage is recorded in multiple currencies.

Using the CurrencyLayer API:

1. Fetch historical exchange rates for:  
   * ILS  
   * EUR  
   * GBP  
   * For a specific historical date  
2. Store the rates in the database.

**Requirements**

* Design the database schema.  
* Prevent duplicate FX records.  
* Handle API failures safely (e.g., network errors, retries).  
* Keep configuration and secrets outside of source code.  
* Maintain stateless behavior.

**In your explanation, describe:**

* How retries are handled safely.  
* How idempotency is guaranteed.  
* What happens if the process crashes during rate insertion.

---

**Task 4 – Production Behavior & Data Observability**

Assume this system runs daily in production.

**Part A – Operational Behavior**

Describe how your system behaves in the following situations:

1. The process receives a SIGTERM during ingestion.  
2. A database connection drops mid-transaction.  
3. Two instances attempt to process the same logical dataset simultaneously.

Explain:

* What guarantees your system provides.  
* How consistency is preserved.  
* Why your design remains safe under these conditions.

---

**Part B – Data Observability**

After deployment, the business reports inconsistencies in revenue calculations.

1. What signals would help you detect data issues?  
2. What metrics would you expose from your system?  
3. How would you detect:  
   * Missing data  
   * Duplicate data  
   * Unexpected value changes  
4. Where in your system would these checks and signals be generated?

You are not required to implement monitoring infrastructure.  
Focus on what should be measured and how your system enables visibility into data correctness.

---

**Submission Guidelines**

* Keep the implementation clean and well-structured.  
* Focus on design quality rather than overengineering.  
* Provide short explanations where requested.  
* Include instructions to run the solution.