# Demo notes

## helpfull scripts to load filed for posrgres visability

> sudo cp "
> /home/karina/PycharmProjects/EPAM/Karyna_Ohol_SP25/STAGE2/Final_Task/source_files/source_system_1_oms_incremental.csv"
> /var/lib/postgresql/16/main/source_system_1_oms_incremental.csv
>
> sudo chown postgres:postgres /var/lib/postgresql/16/main/source_system_1_oms_incremental.csv
>
> sudo chmod 644 /var/lib/postgresql/16/main/source_system_1_oms_incremental.csv
>
> sudo cp "
> /home/karina/PycharmProjects/EPAM/Karyna_Ohol_SP25/STAGE2/Final_Task/source_files/source_system_2_lms_incremental.csv"
> /var/lib/postgresql/16/main/source_system_2_lms_incremental.csv
>
>sudo chown postgres:postgres /var/lib/postgresql/16/main/source_system_2_lms_incremental.csv
>
> sudo chmod 644 /var/lib/postgresql/16/main/source_system_2_lms_incremental.csv
>
> sudo ls -la /var/lib/postgresql/16/main/ | grep csv
>
> sudo rm -f /var/lib/postgresql/16/main/name.csv

> rename files
> sudo mv /var/lib/postgresql/16/main/source_system_1_oms_part1.csv
> /var/lib/postgresql/16/main/source_system_1_oms_full.csv
>
> sudo mv /var/lib/postgresql/16/main/source_system_2_lms_part1.csv
> /var/lib/postgresql/16/main/source_system_2_lms_full.csv

### DWH Initial Setup

OMS (Order Management): 500,392 rows with order/customer/product details

LMS (Logistics Management): 500,392 rows with shipping/delivery details

Relationship: Connected via OrderID=ShipmentID, CustomerID, ProductID

- OrderLines defined as distinct concat(order_src_id,'|',product_src_id,'|',customer_src_id) 443303 (57089 duplicates)
- SipmentLines defined as distinct concat(shipment_src_id,'|',product_src_id,'|',customer_src_id) 443303

Date range: 2023-06 - 2025-06


---

### Increment files Scenarios :

> Target: 5% volume = ~25,020 rows each for incremental files

- New Business → New OrderIDs (July 2025 orders+ historical orders)
- System Updates → Product rebranding, customer email changes, add new cities
- ETL Retries → Duplicate records with same OrderIDs, Orphaned orders without deliveries

### Basic Statistics:

> **OMS records:** 25,000

> **LMS records:** 25,000

> **Unique OMS IDs**: 13,513 --> ce_orders

> **Unique LMS IDs:** 13,513 ---> ce_shipments

> Perfect matches **(ID|ProductID|CustomerID)**: 22,618

> **order_lines** 22706 : 22060 shipped + 646 unshipped
>
> **shipment_lines**  22618

---

### PRODUCT DATA CHANGES IN INCREMENT FILES

> ProductID- 58 name updated - 4 catgories updated - 6 status changes from source

**Selected 58 products for name update:**

Product IDs: ['19', '24', '37', '44', '58', '78', '116',
'127', '134', '191', '203', '216', '249',
'251', '273', '282', '295', '306', '359', '364',
'365', '565', '607', '625', '642', '773', '786',
'793', '804', '810', '818', '821', '823', '825',
'828', '835', '858', '860', '885', '886', '905',
'906', '917', '924', '957', '977', '981', '982',
'1347', '1348', '1349', '1350', '1351', '1352', '1353',
'1356', '1357', '1360', '1362', '1363']

**Selected 4 products for category change:**

Product ID 258: Current category = Boxing & MMA
Product ID 278: Current category = Electronics
Product ID 311: Current category = Kids' Golf Clubs
Product ID 792: Current category = Golf Balls

**6 status changes from source**
305, 564, 172, 1358, 572, 897

---

### CUSTOMER DATA CHANGES IN INCREMENT FILES

```
Checking OMS vs LMS CustomerID consistency...
⚠ 111 customers exist in OMS but not in LMS
Total unique customers in OMS: 8,653
Total unique customers in LMS: 8,542
Customers with updated emails: 2,000  
Records with updated emails: 5,691
```

**Generated new customers** ✓ Generated 20 new customers

``` 
New Customer 20758: Margaret Johnson (F, 1951, Home Office)
New Customer 20759: Donald Walker (M, 1965, Consumer)
New Customer 20760: Nancy Rhodes (F, 1997, Consumer)
New Customer 20761: Stephanie Miller (F, 1987, Corporate)
New Customer 20762: Jennifer Johnson (F, 1951, Consumer)
New Customer 20763: Colleen Wagner (F, 1964, Home Office)
New Customer 20764: Alyssa Gonzalez (F, 1985, Consumer)
New Customer 20765: Matthew Gardner (M, 1964, Corporate)
New Customer 20766: Daniel Lawrence (M, 2001, Consumer)
New Customer 20767: Rhonda Smith (F, 1994, Corporate)
New Customer 20768: Robert Wolfe (M, 1967, Consumer)
New Customer 20769: Gina Moore (F, 1998, Corporate)
New Customer 20770: Tina Rogers (F, 1955, Corporate)
New Customer 20771: Anna Davis (F, 1972, Corporate)
New Customer 20772: Ryan Munoz (M, 2001, Consumer)
New Customer 20773: Robert Blair (M, 1984, Consumer)
New Customer 20774: Joshua Dudley (M, 1955, Home Office)
New Customer 20775: James Arnold (M, 2003, Home Office)
New Customer 20776: Ronald Montgomery (M, 1986, Consumer)
New Customer 20777: Shannon Ray (F, 1952, Home Office)
```

**Sample email updates:**

```
Customer 737 (Lawrence Smith): lawrence.smith.updated@mail.com
Customer 2367 (Sean Smith): sean.smith.updated@mail.com
Customer 6353 (Mary Rogers): mary.rogers.updated@mail.com
Customer 6696 (Mary Spencer): mary.spencer.updated@mail.com
Customer 9304 (Janet Smith): janet.smith.updated@mail.com
```

---

### GEOGRAPHY DATA EXPANTION IN INCREMENT FILES

✓ Created 5 new geography combinations - Will assign each new geography to approximately 100 records

**New Geography combinations:**

```
New Geography 1: Franklin, NY, Puerto Rico
New Geography 2: Georgetown, NC, EE. UU.
New Geography 3: Lincoln, MD, Puerto Rico
New Geography 4: Jefferson, AL, Puerto Rico
New Geography 5: Roosevelt, AR, Puerto Rico
```

---

### REARENGED DATES IN INCREMENT FILES + 6,250 orders (25% of 25,000) to July 2025...

> **New date ranges:**
> OMS OrderDate: 2023-06-13 00:00:00 to 2025-07-25 00:00:00
>
>LMS DeliveryDate: 2023-06-20 00:00:00 to 2030-07-16 00:00:00



**AFTER TRANSFER - Monthly Distribution:**

```
OrderDate
2023-06 480
2023-07 761
2023-08 725
2023-09 714
2023-10 768
2023-11 782
2023-12 846
2024-01 855
2024-02 736
2024-03 821
2024-04 761
2024-05 843
2024-06 712
2024-07 818
2024-08 757
2024-09 771
2024-10 803
2024-11 777
2024-12 782
2025-01 850
2025-02 715
2025-03 792
2025-04 787
2025-05 808
2025-06 286
2025-07 6250
```

--- 

### EXPECTED count of values AFTER increment load

| layer | table_name                        | table_type    | b.rows | added in increment file | after increment | note                                                                                      |
|:------|:----------------------------------|:--------------|:-------|:------------------------|:----------------|:------------------------------------------------------------------------------------------|
| SA    | sa\_lms.src\_lms                  | Source        | 500392 | 25000                   | 525392          |                                                                                           |
| SA    | sa\_oms.src\_oms                  | Source        | 500392 | 25000                   | 525392          |                                                                                           |
| SA    | distinct\_brands\_oms             | Virtual\_Dim  | 59     | 0                       | 59              |                                                                                           |
| SA    | distinct\_carriers\_lms           | Virtual\_Dim  | 4      | 0                       | 4               |                                                                                           |
| SA    | distinct\_categories\_oms         | Virtual\_Dim  | 51     | 0                       | 51              |                                                                                           |
| SA    | distinct\_cities\_lms             | Virtual\_Dim  | 593    | 5                       | 593             |                                                                                           |
| SA    | distinct\_countries\_lms          | Virtual\_Dim  | 2      | 2                       | 2               |                                                                                           |
| SA    | distinct\_customers\_combined     | Virtual\_Dim  | 20469  | 69                      | 20538           |       20 new inserted 49 source                                                                                     |
| SA    | distinct\_delivery\_statuses\_lms | Virtual\_Dim  | 2      | 0                       | 2               |                                                                                           |
| SA    | distinct\_departments\_oms        | Virtual\_Dim  | 11     | 0                       | 11              |                                                                                           |
| SA    | distinct\_geographies\_lms        | Virtual\_Dim  | 593    | 0                       | 593             | 5 new cities expand geography                                                             |
| SA    | distinct\_order\_statuses\_oms    | Virtual\_Dim  | 3      | 0                       | 3               |                                                                                           |
| SA    | distinct\_payment\_methods\_oms   | Virtual\_Dim  | 4      | 0                       | 4               |                                                                                           |
| SA    | distinct\_products\_combined      | Virtual\_Dim  | 118    | 68                      | 118             | changes seen in SCD2 58 name updated - 4 catgories updated - 6 status changes from source |
| SA    | distinct\_product\_statuses\_oms  | Virtual\_Dim  | 5      | 0                       | 5               |                                                                                           |
| SA    | distinct\_sales\_reps\_oms        | Virtual\_Dim  | 100    | 0                       | 100             |                                                                                           |
| SA    | distinct\_shipping\_modes\_lms    | Virtual\_Dim  | 3      | 0                       | 3               |                                                                                           |
| SA    | distinct\_states\_lms             | Virtual\_Dim  | 46     | 0                       | 46              |                                                                                           |
| SA    | distinct\_warehouses\_lms         | Virtual\_Dim  | 5      | 0                       | 5               |                                                                                           |
| SA    | distinct\_order\_lines\_oms       | Virtual\_Fact | 443303 | 22706                   | 466009          | 22060 shipped + 646 unshipped / duplicates:   57089(init)+ 2294                           |
| SA    | distinct\_orders\_oms             | Virtual\_Fact | 187449 | 13513                   | 200962          |                                                                                           |
| SA    | distinct\_shipment\_lines\_lms    | Virtual\_Fact | 443303 | 22618                   | 465921          | duplicates:   57089(init)+ 2383                                                           |
| SA    | distinct\_shipments\_lms          | Virtual\_Fact | 187449 | 13513                   | 200962          |                                                                                           |




