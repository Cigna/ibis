```
 ▄█  ▀█████████▄   ▄█     ▄████████
███    ███    ███ ███    ███    ███
███▌   ███    ███ ███▌   ███    █▀
███▌  ▄███▄▄▄██▀  ███▌   ███
███▌ ▀▀███▀▀▀██▄  ███▌ ▀███████████
███    ███    ██▄ ███           ███
███    ███    ███ ███     ▄█    ███
█▀   ▄█████████▀  █▀    ▄████████▀

```
# IBIS Tests And Build
Once the initial setup is complete, next step is to run unit tests

## Running Unit tests
To ensure that IBIS can be automatically deployed, has consistency and
reliability, unit tests are a must. The development team follows TDD practices.
Unit tests are supposed to run under 10 - 15 seconds.
To run unit tests, use the following:


```python ibis_test_suite.py```

## Code validation and Build process
To build the code, navigate to the [ibis_build](/ibis_build/) folder and use the following:


```sh build.sh <Argument1: ibis_home> <Argument2 : Execution option>```

Argument 1 : Needs to be IBIS home directory path

Argument 2 : Below are the acceptable argument values

             No arguments or leaving Blank will execute Test cases, Code check and Build process.
             skip-all-test - Skip all validation and create Egg
             skip-code-check - Skip code style check
             skip-build - Run all validations and skip build process
             

In the ibis_build folder, it would now create the egg, coverage report and the 
packaged ibis.tar.gz file containing egg with required IBIS supporting files