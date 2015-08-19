Rack-Scale
==========

Each node contains CPU, memory, and disk.

Flow Sizes
==========

Memory Only
-----------

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
![Memory Only](./nic_memflowsizes_cdf.png)|![Memory Only](./rack-scale_plainmemflowsizes_cdf.png)|![Memory Only](./rack-scale_combinedmemflowsizes_cdf.png)|![Memory Only](./rack-scale_timeonlymemflowsizes_cdf.png)

Disk Only
---------

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
![Disk Only](./nic_diskflowsizes_cdf.png)|![Disk Only](./rack-scale_plaindiskflowsizes_cdf.png) |![Disk Only](./rack-scale_combineddiskflowsizes_cdf.png) |![Disk Only](./rack-scale_timeonlydiskflowsizes_cdf.png)

All Flows
---------

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
![All](./nic_allflowsizes_cdf.png) | ![All](./rack-scale_plainallflowsizes_cdf.png)        |![All](./rack-scale_combinedallflowsizes_cdf.png)        |![All](./rack-scale_timeonlyallflowsizes_cdf.png)

Interarrivals
=============

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
 ![Interarrival CDF Per Source](./nic_comparefit_cdf_src_interarrivals.png)|![Interarrival CDF Per Source](./rack-scale_plaincomparefit_cdf_src_interarrivals.png)| ![Interarrival CDF Per Source](./rack-scale_combinedcomparefit_cdf_src_interarrivals.png)|![Interarrival CDF Per Source](./rack-scale_timeonlycomparefit_cdf_src_interarrivals.png)
 ![PDF of Interarrival Times](./nic_pdf_src_interarrivals.png)             |![PDF of Interarrival Times](./rack-scale_plainpdf_src_interarrivals.png)             | ![PDF of Interarrival Times]  (./rack-scale_combinedpdf_src_interarrivals.png)             |![PDF of Interarrival Times](./rack-scale_timeonlypdf_src_interarrivals.png)             

Traffic Volume
==============

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
![Traffic Volume in 1ms measurement increments](./nic_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./rack-scale_plaintrafficvolume.png)      | ![Traffic Volume in 1ms measurement increments](./rack-scale_combinedtrafficvolume.png)|![Traffic Volume in 1ms measurement increments](./rack-scale_timeonlytrafficvolume.png)
 ![Traffic Volume in 1ms measurement increments](./nic_cdf_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./rack-scale_plaincdf_trafficvolume.png)  | ![Traffic Volume in 1ms measurement increments](./rack-scale_combinedcdf_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./rack-scale_timeonlycdf_trafficvolume.png)
 ![Traffic Volume in 1ms measurement increments](./nic_derivative_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./rack-scale_plainderivative_trafficvolume.png)  | ![Traffic Volume in 1ms measurement increments](./rack-scale_combinedderivative_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./rack-scale_timeonlyderivative_trafficvolume.png)


Resource-Based
==========

Each node contains *either* CPU, memory, *or* disk. (There are more nodes in the network overall).

Flow Sizes
==========

Memory Only
-----------

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
![Memory Only](./nic_memflowsizes_cdf.png)|![Memory Only](./res-based_plainmemflowsizes_cdf.png)|![Memory Only](./res-based_combinedmemflowsizes_cdf.png)|![Memory Only](./res-based_timeonlymemflowsizes_cdf.png)

Disk Only
---------

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
![Disk Only](./nic_diskflowsizes_cdf.png)|![Disk Only](./res-based_plaindiskflowsizes_cdf.png) |![Disk Only](./res-based_combineddiskflowsizes_cdf.png) |![Disk Only](./res-based_timeonlydiskflowsizes_cdf.png)

All Flows
---------

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
![All](./nic_allflowsizes_cdf.png) | ![All](./res-based_plainallflowsizes_cdf.png)        |![All](./res-based_combinedallflowsizes_cdf.png)        |![All](./res-based_timeonlyallflowsizes_cdf.png)

Interarrivals
=============

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
 ![Interarrival CDF Per Source](./nic_comparefit_cdf_src_interarrivals.png)|![Interarrival CDF Per Source](./res-based_plaincomparefit_cdf_src_interarrivals.png)| ![Interarrival CDF Per Source](./res-based_combinedcomparefit_cdf_src_interarrivals.png)|![Interarrival CDF Per Source](./res-based_timeonlycomparefit_cdf_src_interarrivals.png)
 ![PDF of Interarrival Times](./nic_pdf_src_interarrivals.png)             |![PDF of Interarrival Times](./res-based_plainpdf_src_interarrivals.png)             | ![PDF of Interarrival Times]  (./res-based_combinedpdf_src_interarrivals.png)             |![PDF of Interarrival Times](./res-based_timeonlypdf_src_interarrivals.png)             

Traffic Volume
==============

Pre-Disaggregation | Uncombined | Combined |Combined on Time Only
-------------------|------------|----------|---------------------
![Traffic Volume in 1ms measurement increments](./nic_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_plaintrafficvolume.png)      | ![Traffic Volume in 1ms measurement increments](./res-based_combinedtrafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_timeonlytrafficvolume.png)
 ![Traffic Volume in 1ms measurement increments](./nic_cdf_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_plaincdf_trafficvolume.png)  | ![Traffic Volume in 1ms measurement increments](./res-based_combinedcdf_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_timeonlycdf_trafficvolume.png)
 ![Traffic Volume in 1ms measurement increments](./nic_derivative_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_plainderivative_trafficvolume.png)  | ![Traffic Volume in 1ms measurement increments](./res-based_combinedderivative_trafficvolume.png)|![Traffic Volume in 1ms measurement increments](./res-based_timeonlyderivative_trafficvolume.png)

