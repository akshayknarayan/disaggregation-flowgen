Resource-Based
==========

Each node contains *either* CPU, memory, *or* disk. (There are more nodes in the network overall).

Flow Sizes
==========

Memory Only
-----------

Pre-Disaggregation | Uncombined |Combined on Time Only
-------------------|------------|---------------------
![Memory Only](./nic_memflowsizes_cdf.png)|![Memory Only](./res-based_plain_memflowsizes_cdf.png)|![Memory Only](./res-based_timeonly_memflowsizes_cdf.png)

Disk Only
---------

Pre-Disaggregation | Uncombined | Combined on Time Only
-------------------|------------|---------------------
![Disk Only](./nic_diskflowsizes_cdf.png)|![Disk Only](./res-based_plain_diskflowsizes_cdf.png) |![Disk Only](./res-based_timeonly_diskflowsizes_cdf.png)

All Flows
---------

Pre-Disaggregation | Uncombined | Combined on Time Only
-------------------|------------|---------------------
![All](./nic_allflowsizes_cdf.png) | ![All](./res-based_plain_allflowsizes_cdf.png) |![All](./res-based_timeonly_allflowsizes_cdf.png)

Interarrivals
=============

Pre-Disaggregation | Uncombined | Combined on Time Only
-------------------|------------|---------------------
 ![Interarrival CDF Per Source](./nic_comparefit_cdf_src_interarrivals.png)|![Interarrival CDF Per Source](./res-based_plain_comparefit_cdf_src_interarrivals.png)|![Interarrival CDF Per Source](./res-based_timeonly_comparefit_cdf_src_interarrivals.png)
 ![PDF of Interarrival Times](./nic_pdf_src_interarrivals.png)             |![PDF of Interarrival Times](./res-based_plain_pdf_src_interarrivals.png)             | ![PDF of Interarrival Times](./res-based_timeonly_pdf_src_interarrivals.png)             

Traffic Volume
==============

Pre-Disaggregation | Uncombined |Combined on Time Only
-------------------|------------|---------------------
![Traffic Volume in 1ms measurement increments](./nic_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_plain_trafficvolume.png)      | ![Traffic Volume in 1ms measurement increments](./res-based_timeonly_trafficvolume.png)
 ![Traffic Volume in 1ms measurement increments](./nic_cdf_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_plain_cdf_trafficvolume.png)  | ![Traffic Volume in 1ms measurement increments](./res-based_timeonly_cdf_trafficvolume.png)
 ![Traffic Volume in 1ms measurement increments](./nic_derivative_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_plain_derivative_trafficvolume.png)  | ![Traffic Volume in 1ms measurement increments](./res-based_timeonly_derivative_trafficvolume.png)
