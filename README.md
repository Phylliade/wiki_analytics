# Wiki analytics #

# Installation
After downloading this repository, there is no install required for the script itself, but you must install the dependencies by running this command :

```
pip install -r requirements.txt
```

# Running the script
Go to `src` and then run (with Python3) the script `hourly_analytics`

```
cd src && python3 hourly_analytics.py
```

This will run the script with the current date

## Custom dates
If you want to call the script for custom dates and hours, call the function `hourly_analytics` of the module of the same name (`hourly_analytics`) and give it a list of Python `datetime`.
