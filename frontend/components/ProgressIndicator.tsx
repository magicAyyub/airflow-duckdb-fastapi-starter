"use client";

import React from 'react';
import { CheckCircle, Loader2 } from 'lucide-react';


const ProgressIndicator = ({ progress, stage }) => {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <span className="text-lg font-semibold">{stage}</span>
        <span className="text-lg font-semibold">{progress}%</span>
      </div>
      <div className="w-full bg-gray-200 rounded-full h-2.5">
        <div
          className="bg-black h-2.5 rounded-full transition-all duration-500 ease-out"
          style={{ width: `${progress}%` }}
        ></div>
      </div>
      <div className="flex items-center justify-center">
        {progress < 100 ? (
          <Loader2 className="w-8 h-8 text-black animate-spin" />
        ) : (
          <CheckCircle className="w-8 h-8 text-green-500" />
        )}
      </div>
      <p className="text-center text-gray-600">
        {progress < 100
          ? "Traitement en cours, veuillez patienter..."
          : "Traitement terminé !"}
      </p>
    </div>
  );
};

export default ProgressIndicator;