const path = require('path');

module.exports = {
    target: 'node',
    entry: './src/index.ts',
    module: {
        rules: [
            {
                test: /\.ts$/,
                use: 'ts-loader',
                include: [
                    path.resolve(__dirname, 'src'),
                ],
                exclude: /node_modules/,
            }
        ]
    },
    resolve: {
        extensions: ['.ts', '.js'],
        fallback: {
            buffer: require.resolve('buffer/'),
            util: require.resolve("util/"),
        }
    },
    output: {
        filename: 'index.js',
        path: path.resolve(__dirname, 'build'),
        libraryTarget: 'commonjs2',
    },
    optimization: {
        minimize: false,
    },
}
