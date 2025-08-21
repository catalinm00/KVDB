import http from 'k6/http';
import { check } from 'k6';

export const options = {
    vus: 70,       // más usuarios virtuales para saturar
    duration: '45s' // tiempo total de prueba
};

function generateRandomKey(length) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

export default function () {
    const url = 'http://localhost:3070/api/db';
    const payload = JSON.stringify({
        key: generateRandomKey(6),
        value: 'foobar'
    });

    const params = {
        headers: {
            'Content-Type': 'application/json'
        }
    };

    const res = http.post(url, payload, params);

    check(res, {
        'status es 200': (r) => r.status === 200
    });
}
