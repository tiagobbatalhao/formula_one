.PHONY: clean
clean:
	find . | grep -E "(__pycache__)" | xargs rm -rf
	find . | grep -E "(\.pyc$)" | xargs rm -rf
	find . | grep -E "(.ipynb_checkpoints)" | xargs rm -rf