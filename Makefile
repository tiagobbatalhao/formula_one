.PHONY: clean
clean:
	find . | grep -E "(__pycache__)" | xargs rm -rf
	find . | grep -E "(\.pyc$)" | xargs rm -rf
	find . | grep -E "(.ipynb_checkpoints)" | xargs rm -rf

.PHONY: load_races
load_races:
	python formula_one/load_to_big_query/load_races.py --year_min 1950 --year_max 2023

.PHONY: load_sessions
load_sessions:
	python formula_one/load_to_big_query/load_session_data.py --year ${YEAR}
