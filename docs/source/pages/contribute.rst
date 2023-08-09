.. _contribute:


Development
~~~~~~~~~~~

Initialize the development environment installing dev dependencies:

.. code-block:: console

    $ poetry install --no-root


Code style
__________

After any code changes make sure that code style is followed.
To control that automatically install pre-commit hooks:

.. code-block:: console

    $ pre-commit install

It will be checking your changes for coding conventions used in the project before any commit.


Pull Requests
_____________

To contribute checkout branch ``dev``, create a feature branch and make pull request setting
``dev`` as a target.


Documentation
_____________

If you've made any changes to the documentation, make sure it builds successfully.
To build the documentation follow the instructions:

- Install documentation generation dependencies:

.. code-block:: console

    $ poetry install -E docs

- Build the documentation:

.. code-block:: console

    $ cd docs
    $ make html
