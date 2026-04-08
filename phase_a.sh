#!/bin/bash

############################################################################################
# High-level overview of log stream (Top 5 airlines by events + revenue)                   #
############################################################################################

# set -x

# Check input argument
if [[ -z "$1" ]]; then
    echo "Usage: ./phase_a.sh <input_file>"
    exit 1
fi

input_file="$1"

# Check if file exists
if [[ ! -f "$input_file" ]]; then
    echo "File not found!"
    exit 1
fi

# Create output folder if not exists
output_dir="outputs"
mkdir -p "$output_dir"

# Generate output filename with date
current_date=$(date +"%Y-%m-%d_%H-%M-%S")
output_file="$output_dir/top_airlines_revenue_$current_date.csv"

echo "Processing file: $input_file"
echo "Output will be saved to: $output_file"

echo "Processing the stream logs json file..."

# Process data
jq -r '
select(.airline? and .price? and .booking_id?) |
[
  (.airline | ascii_upcase),
  (
    if (.price | type) == "object" then .price.amount
    else .price
    end
  ) // 0
]
| @tsv
' "$input_file" 2>/dev/null |
awk -F'\t' '
{
    airline = $1
    price = $2 + 0

    count[airline]++
    revenue[airline] += price
}
END {
    for (a in count) {
        printf "%s\t%d\t%.2f\n", a, count[a], revenue[a]
    }
}
' |
sort -k2 -nr |
head -5 |
awk 'BEGIN {
    print "airline,events,revenue"
}
{
    printf "%s,%d,%.2f\n", $1, $2, $3
}' > "$output_file"

echo "Processing complete !"
echo "Output saved at: $output_file"