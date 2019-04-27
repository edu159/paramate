====================================
Paramate documentation
====================================

The ``paramate`` package simplifies the creation  and execution of sofisticated 
parameter studies. It is thought to be employed, although not exclusively, 
to interact with clusters with `job schedulers <https://en.wikipedia.org/wiki/Job_scheduler>`_ 
like PBS, SLURM, SGE, etc. The software can be used in a self-contained manner 
without the need of admin privileges. To find if this software is actually useful 
to you, jump straight to the :doc:`simple example <simple_example>` section.


.. note::
    
    ``paramate`` lacks any client-server structure and all the actions are performed
    through remotely executed commands over ``SSH`` and text processing on response. 
    This is very convenient as no software or configuration is needed server-side. Your
    local cluster administrators will be full of joy!


------------------------------------
Installing
------------------------------------

The package is distributed on PyPI and can be installed with pip:

.. code-block:: shell

    pip install paramate

For more information on installation and ``paramate`` dependencies go to :doc:`here <installation>`

------------------------------------
Quickstart
------------------------------------

To quickly create a **study** execute:

.. code-block:: shell

    paramate create -n myfirst_study

A directory structure inside ``my_first_study`` is created in your working directory. There should be
three files inside called ``params.yaml``, ``remotes.yaml`` and ``generators.py``, which are core to 
``parampy``. Also a directory named ``template`` contains a proposed (but not mandatory) structure for
each **case**. The purpose of each file will be clarified in the :doc:`simple example <simple_example>` section.

.. note::
    
    A **case** is an instance of a parameter **study** and it is generated from the ``template`` directory.
    For a complete list of the terms employed in ``parampy`` go to the :doc:`glossary <glossary>`.


.. toctree::
   :maxdepth: 2
   :caption: GETTING STARTED 

   about
   installation
   glossary
   simple_example



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
