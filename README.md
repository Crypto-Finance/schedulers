# schedulers
This repo is used for the schedulers necessary for the TradingBot 

## Setting up the project
---
### **Step 1) Create a virtual enviroment.**
On a Terminal, install the package for python virtual enviroment using pip.
```bash
python -m pip install virtualenv
```

Set up the virtual enviroment in the folder named **pyvirtual**
```bash
python -m virtualenv venv
```
Activate the virtual env in the terminal to be used for execution:

- *on Linux*
```bash
source venv/bin/activate
```
* *on windows*
```bash
venv/bin/activate
```

### **Step 2) Install all the required dependencies.**
Once the virtual environment is activated, proceed to install all the required packages. pip offers a way to automatically install all the dependencies used with a requirements file.

To install the requirements, follow:
```bash
python -m pip install -r requirements.txt
```

If a new dependency is added to the code, an update to the requirements is required. To do so follow:
```bash
python -m pip-chill > requirements.txt
```