Collective Intelligence in League of Legends

Steps for reproducing the results --
1. Create and activate virtual environment for the repository using the commands `python3 -m venv .` and `source bin/activate`
2. Install dependencies from the requirements.txt file using the command `python3 -m pip install -r requirements.txt`
3. Setup a developers account at Riot Games to use their RESTful APIs through an API key. Create a .env file as shown in sample.env file, and fill in the appropriate values from the developers account
4. Run the `gather_dataset.py` script to fetch API data. Requires creation of certain folders. For example -- `matchData`, `matchTimeline`, `ranks` in the root directory
5. Navigate to the notebooks folder to run analyses on the data 
