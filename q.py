import pandas as pd

# Data initialization
inward_qty = [2221.3, 1009.25, 2019.55, 2000.93, 1991.45, 2009.6, 3386.7, 3143.73, 3417.85, 3525.65, 2280.5, 2259.7, 906.15, 1101.25]
inward_batch = ['KBB/305/0723/01', 'KBB/305/0723/01', 'KBB/305/0723/01', 'KBB/305/0723/01', 'KBB/305/0723/01', 'KBB/305/0723/01',
                'KBB/305/0923/01', 'KBB/305/0923/01', 'KBB/305/1023/01', 'KBB/305/1023/01', 'KBB/305/1023/01', 'KBB/305/1023/01',
                'KBB/305/1023/01', 'KBB/305/1023/01']
used_qty = [70, 208, 234, 156, 208, 208, 156, 208, 182, 234, 104, 234, 234, 260, 260, 260, 234, 234, 234, 208, 260, 260, 156, 208, 208, 260, 234, 208, 234, 182, 260, 208, 104, 46, 184, 230, 230, 138, 207, 230, 230, 184, 253, 276, 276, 230, 230, 276, 276, 253, 207, 207, 230, 211, 257, 253, 230, 276, 138, 184, 184, 330, 297, 198, 165, 330, 297, 330, 297, 230, 230, 230, 330, 330, 330, 330, 330, 330, 330, 253, 230, 299, 297, 138, 299, 230, 330, 330, 330, 230, 207, 161, 184, 161, 330, 330, 330, 330, 260, 253, 230, 207, 253, 253, 276, 276, 230, 363, 396, 330, 330, 230, 184, 184, 115, 297, 330, 330, 184, 207, 115, 230, 207, 184, 138]

# Lists to store results
inward_qty_result = []
inward_batch_result = []
used_qty_result = []
used_batch_result = []

# Initialize pointers
inward_pointer = 0
inward_qty_remaining = inward_qty[inward_pointer]

# Process FIFO
for uq in used_qty:
    allocated_batches = []
    while uq > 0 and inward_pointer < len(inward_qty):
        if inward_qty_remaining <= uq:
            uq -= inward_qty_remaining
            allocated_batches.append(inward_batch[inward_pointer])
            # Store the results
            inward_qty_result.append(inward_qty_remaining)
            inward_batch_result.append(inward_batch[inward_pointer])
            used_qty_result.append(inward_qty_remaining)
            used_batch_result.append(', '.join(allocated_batches))
            inward_pointer += 1
            if inward_pointer < len(inward_qty):
                inward_qty_remaining = inward_qty[inward_pointer]
        else:
            inward_qty_remaining -= uq
            allocated_batches.append(inward_batch[inward_pointer])
            # Store the results
            inward_qty_result.append(uq)
            inward_batch_result.append(inward_batch[inward_pointer])
            used_qty_result.append(uq)
            used_batch_result.append(', '.join(allocated_batches))
            uq = 0

# Convert lists to DataFrame
df = pd.DataFrame({
    'Inward Qty': inward_qty_result,
    'Inward Batch': inward_batch_result,
    'Used Qty': used_qty_result,
    'Used Batch': used_batch_result
})

df.to_excel(r"D:\UserProfile\Desktop\Batch_data.xlsx",index=False)
