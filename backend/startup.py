#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fast startup wrapper for Cloud Run
Starts uvicorn without importing heavy dependencies upfront
"""

import os
import sys

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8080))
    
    # Start server with lazy loading
    uvicorn.run(
        "app_gcp:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
        access_log=True
    )
