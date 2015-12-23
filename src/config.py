import logging

# Timezone.
# Please indicate a valid int
# Ex : For Europe/Paris : 1
local_timezone = 1
# Path of the resources dir
resource_dir = "../resources"

# Path of the output files
output_dir = "../output"

# URL of the balcklist file
blacklist_origin = "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages"

# Path of the blacklist file
blacklist_path = resource_dir + "/" + "blacklist_domains_and_pages"

# Log level
log_level = logging.DEBUG

# Output the result as pure csv or output a fancy result
fancy_formatting = False
