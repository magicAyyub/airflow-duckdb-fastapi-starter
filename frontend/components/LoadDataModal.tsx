"use client";

import React, { useState } from 'react';
import { Database, Loader, CheckCircle, AlertCircle } from 'lucide-react';
import ProgressIndicator from './ProgressIndicator';

const Modal = ({ isOpen, onClose, children, title }) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="fixed inset-0 bg-black bg-opacity-50 backdrop-blur-sm transition-opacity"></div>
      <div className="flex min-h-full items-center justify-center p-4">
        <div className="relative w-full max-w-lg transform overflow-hidden rounded-xl bg-white shadow-2xl transition-all">
          <div className="flex items-center justify-between border-b border-gray-100 p-4">
            <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
          </div>
          <div className="p-6">{children}</div>
        </div>
      </div>
    </div>
  );
};

const LoadDataModal = ({ isOpen, onClose }) => {
  const [tableName, setTableName] = useState('');
  const [status, setStatus] = useState('input');
  const [error, setError] = useState('');
  const [errorDetails, setErrorDetails] = useState('');
  const [progress, setProgress] = useState(0);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!tableName.trim()) {
      setError('Le nom de la table est requis');
      return;
    }

    setStatus('loading');
    setError('');
    setErrorDetails('');
    const formData = new FormData();
    formData.append('table_name', tableName);

    try {
      const response = await fetch('/api/load_data', {
        method: 'POST',
        body: formData
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Erreur lors du chargement');
      }

      setStatus('success');
    } catch (err) {
      console.error(err);
      setError(err.message);
      setErrorDetails(err.details || '');
      setStatus('error');
    }
  };

  const handleClose = () => {
    setStatus('input');
    setTableName('');
    setError('');
    setErrorDetails('');
    setProgress(0);
    onClose();
  };

  const renderContent = () => {
    switch (status) {
      case 'loading':
        return (
          <div className="flex flex-col items-center py-8">
            <ProgressIndicator progress={progress} stage={`Chargement des données: ${progress}%`} />
          </div>
        );

      case 'success':
        return (
          <div className="flex flex-col items-center py-8">
            <CheckCircle className="h-12 w-12 text-green-500 mb-4" />
            <h4 className="text-xl font-semibold text-gray-900 mb-2">Chargement réussi !</h4>
            <p className="text-gray-600 text-center">
              Les données ont été chargées avec succès dans la table {tableName}
            </p>
            <button
              onClick={handleClose}
              className="mt-6 px-4 py-2 bg-black text-white rounded-lg hover:bg-gray-800 transition-colors"
            >
              Fermer
            </button>
          </div>
        );

      case 'error':
        return (
          <div className="flex flex-col items-center py-8">
            <AlertCircle className="h-12 w-12 text-red-500 mb-4" />
            <h4 className="text-xl font-semibold text-gray-900 mb-2">
              Une erreur est survenue
            </h4>
            <p className="text-red-600 text-center mb-2">{error}</p>
            {errorDetails && (
              <details className="mt-2 text-sm text-gray-600">
                <summary>Détails de l'erreur</summary>
                <pre className="mt-2 whitespace-pre-wrap">{errorDetails}</pre>
              </details>
            )}
            <button
              onClick={() => setStatus('input')}
              className="mt-4 px-6 py-2 bg-black text-white rounded-lg hover:bg-gray-800 transition-colors"
            >
              Réessayer
            </button>
          </div>
        );

      default: // 'input'
        return (
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Nom de la table (mis à jour si elle existe)
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <Database className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  type="text"
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  className="block w-full pl-10 pr-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-black focus:border-black"
                  placeholder="Entrez le nom de la table"
                />
              </div>
              {error && (
                <p className="mt-1 text-sm text-red-600">{error}</p>
              )}
            </div>

            <div className="flex justify-end space-x-3 mt-6">
              <button
                type="button"
                onClick={handleClose}
                className="px-4 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50 transition-colors"
              >
                Annuler
              </button>
              <button
                type="submit"
                className="px-4 py-2 bg-black text-white rounded-md hover:bg-gray-800 transition-colors"
              >
                Charger les données
              </button>
            </div>
          </form>
        );
    }
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={status !== 'loading' ? handleClose : undefined}
      title={
        status === 'loading' ? "Chargement des données" :
        status === 'success' ? "Opération réussie" :
        status === 'error' ? "Erreur" :
        "Charger les données dans la base"
      }
    >
      {renderContent()}
    </Modal>
  );
};

export default LoadDataModal;